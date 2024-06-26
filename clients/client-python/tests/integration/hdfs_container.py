"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import asyncio
import logging
import time

import docker
from docker import types as tp
from docker.errors import NotFound

from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException

logger = logging.getLogger(__name__)


async def check_hdfs_status(hive_container):
    retry_limit = 15
    for _ in range(retry_limit):
        try:
            command_and_args = ["bash", "/tmp/check-status.sh"]
            exec_result = hive_container.exec_run(command_and_args)
            if exec_result.exit_code != 0:
                message = (
                    f"Command {command_and_args} exited with {exec_result.exit_code}"
                )
                logger.warning(message)
                logger.warning("output: %s", exec_result.output)
                output_status_command = ["hdfs", "dfsadmin", "-report"]
                exec_result = hive_container.exec_run(output_status_command)
                logger.info("HDFS report, output: %s", exec_result.output)
            else:
                logger.info("HDFS startup success!")
                return True
        except Exception as e:
            logger.error("While checking HDFS container status happen exception: %s", e)
        time.sleep(10)
    return False


async def check_hive_container_status(hive_container):
    timeout_sec = 150
    try:
        result = await asyncio.wait_for(
            check_hdfs_status(hive_container), timeout=timeout_sec
        )
        assert result is True, "HDFS container startup failed!"
    except asyncio.TimeoutError as e:
        raise GravitinoRuntimeException(
            "Timeout occurred while waiting for checking HDFS container status."
        ) from e


class HDFSContainer:
    _docker_client = None
    _container = None
    _network = None
    _ip = ""
    _network_name = "python-net"
    _container_name = "python-hdfs"

    def __init__(self):
        self._docker_client = docker.from_env()
        self._create_networks()
        try:
            container = self._docker_client.containers.get(self._container_name)
            if container is not None:
                if container.status == "running":
                    container.kill()
                container.remove()
        except NotFound:
            logger.warning("Cannot find hdfs container in docker env, skip to remove.")
        self._container = self._docker_client.containers.run(
            image="datastrato/gravitino-ci-hive:0.1.12",
            name=self._container_name,
            detach=True,
            environment={"HADOOP_USER_NAME": "datastrato"},
            network=self._network_name,
        )
        asyncio.run(check_hive_container_status(self._container))

        self._fetch_ip()

    def _create_networks(self):
        pool_config = tp.IPAMPool(subnet="10.20.31.16/28")
        ipam_config = tp.IPAMConfig(driver="default", pool_configs=[pool_config])
        networks = self._docker_client.networks.list()
        for network in networks:
            if network.name == self._network_name:
                self._network = network
                break
        if self._network is None:
            self._network = self._docker_client.networks.create(
                name=self._network_name, driver="bridge", ipam=ipam_config
            )

    def _fetch_ip(self):
        if self._container is None:
            raise GravitinoRuntimeException("The HDFS container has not init.")

        container_info = self._docker_client.api.inspect_container(self._container.id)
        self._ip = container_info["NetworkSettings"]["Networks"][self._network_name][
            "IPAddress"
        ]

    def get_ip(self):
        return self._ip

    def close(self):
        try:
            self._container.kill()
        except RuntimeError as e:
            logger.warning(
                "While killing container %s happen exception: %s",
                self._container_name,
                e,
            )
        try:
            self._container.remove()
        except RuntimeError as e:
            logger.warning(
                "While removing container %s happen exception: %s",
                self._container_name,
                e,
            )
        try:
            self._network.remove()
        except RuntimeError as e:
            logger.warning(
                "While removing network %s happen exception: %s", self._network_name, e
            )


hdfs_container = HDFSContainer()