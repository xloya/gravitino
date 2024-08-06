"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

# pylint: disable=protected-access,too-many-lines,too-many-locals

import base64
import os
import random
import string
import time
import unittest
from unittest.mock import patch

import pandas
import pyarrow as pa
import pyarrow.dataset as dt
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem

from gravitino import gvfs
from gravitino import NameIdentifier
from gravitino.auth.auth_constants import AuthConstants
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.fileset_context_dto import FilesetContextDTO
from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.filesystem.gvfs import StorageType, FilesetContextPair
from gravitino.exceptions.base import GravitinoRuntimeException

from tests.unittests import mock_base


def generate_unique_random_string(length):
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.sample(characters, length))
    return random_string


@mock_base.mock_data
class TestLocalFilesystem(unittest.TestCase):
    _local_base_dir_path: str = "file:/tmp/fileset"
    _fileset_dir: str = (
        f"{_local_base_dir_path}/{generate_unique_random_string(10)}/fileset_catalog/tmp"
    )

    def setUp(self) -> None:
        local_fs = LocalFileSystem()
        if not local_fs.exists(self._fileset_dir):
            local_fs.mkdir(self._fileset_dir)

    def tearDown(self) -> None:
        local_fs = LocalFileSystem()
        if local_fs.exists(self._local_base_dir_path):
            local_fs.rm(self._local_base_dir_path, recursive=True)

    def test_cache(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cache"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cache"
        actual_path = fileset_storage_location
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_cache", f"{self._fileset_dir}/test_cache", actual_path
            ),
        ):
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)
            self.assertTrue(local_fs.exists(fileset_storage_location))

            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri="http://localhost:9090",
                metalake_name="metalake_demo",
                cache_size=1,
                cache_expired_time=1,
            )
            self.assertTrue(fs.exists(fileset_virtual_location))
            # wait 2 seconds
            time.sleep(2)
            self.assertIsNone(fs._cache.get("file:/"))

    def test_simple_auth(self, *mock_methods):
        options = {"auth_type": "simple"}
        current_user = (
            None if os.environ.get("user.name") is None else os.environ["user.name"]
        )
        user = "test_gvfs"
        os.environ["user.name"] = user
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            options=options,
        )
        token = fs._client._rest_client.auth_data_provider.get_token_data()
        token_string = base64.b64decode(
            token.decode("utf-8")[len(AuthConstants.AUTHORIZATION_BASIC_HEADER) :]
        ).decode("utf-8")
        self.assertEqual(f"{user}:dummy", token_string)
        if current_user is not None:
            os.environ["user.name"] = current_user

    def test_ls(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_ls"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_ls"
        actual_path = fileset_storage_location
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_ls", f"{self._fileset_dir}/test_ls", actual_path
            ),
        ):
            local_fs = LocalFileSystem()
            local_fs.mkdir(fileset_storage_location)
            sub_dir_path = f"{fileset_storage_location}/test_1"
            local_fs.mkdir(sub_dir_path)
            self.assertTrue(local_fs.exists(sub_dir_path))
            sub_file_path = f"{fileset_storage_location}/test_file_1.par"
            local_fs.touch(sub_file_path)
            self.assertTrue(local_fs.exists(sub_file_path))

            fs = gvfs.GravitinoVirtualFileSystem(
                server_uri="http://localhost:9090", metalake_name="metalake_demo"
            )
            self.assertTrue(fs.exists(fileset_virtual_location))

            # test detail = false
            file_list_without_detail = fs.ls(fileset_virtual_location, detail=False)
            file_list_without_detail.sort()
            self.assertEqual(2, len(file_list_without_detail))
            self.assertEqual(
                file_list_without_detail[0], f"{fileset_virtual_location}/test_1"
            )
            self.assertEqual(
                file_list_without_detail[1],
                f"{fileset_virtual_location}/test_file_1.par",
            )

            # test detail = true
            file_list_with_detail = fs.ls(fileset_virtual_location, detail=True)
            file_list_with_detail.sort(key=lambda x: x["name"])
            self.assertEqual(2, len(file_list_with_detail))
            self.assertEqual(
                file_list_with_detail[0]["name"], f"{fileset_virtual_location}/test_1"
            )
            self.assertEqual(
                file_list_with_detail[1]["name"],
                f"{fileset_virtual_location}/test_file_1.par",
            )

    def test_info(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_info"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_info"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/test_1"
        local_fs.mkdir(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_info", f"{self._fileset_dir}/test_info", actual_path
            ),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        dir_virtual_path = fileset_virtual_location + "/test_1"
        actual_path = fileset_storage_location + "/test_1"
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_info", f"{self._fileset_dir}/test_info", actual_path
            ),
        ):
            dir_info = fs.info(dir_virtual_path)
            self.assertEqual(dir_info["name"], dir_virtual_path)

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_info", f"{self._fileset_dir}/test_info", actual_path
            ),
        ):
            file_info = fs.info(file_virtual_path)
            self.assertEqual(file_info["name"], file_virtual_path)

    def test_exist(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_exist"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_exist"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_dir_path = f"{fileset_storage_location}/test_1"
        local_fs.mkdir(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_exist", f"{self._fileset_dir}/test_exist", actual_path
            ),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        dir_virtual_path = fileset_virtual_location + "/test_1"
        actual_path = fileset_storage_location + "/test_1"
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_exist", f"{self._fileset_dir}/test_exist", actual_path
            ),
        ):
            self.assertTrue(fs.exists(dir_virtual_path))

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path = fileset_storage_location + "/test_file_1.par"
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_exist", f"{self._fileset_dir}/test_exist", actual_path
            ),
        ):
            self.assertTrue(fs.exists(file_virtual_path))

    def test_cp_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cp_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cp_file"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        with local_fs.open(sub_file_path, "wb") as f:
            f.write(b"test_file_1")

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_cp_file", f"{self._fileset_dir}/test_cp_file", actual_path
            ),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path = fileset_storage_location + "/test_file_1.par"
        src_context = mock_base.mock_get_fileset_context(
            "test_cp_file", f"{self._fileset_dir}/test_cp_file", actual_path
        )
        actual_path1 = fileset_storage_location + "/test_cp_file_1.par"
        dst_context = mock_base.mock_get_fileset_context(
            "test_cp_file", f"{self._fileset_dir}/test_cp_file", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            side_effect=[src_context, src_context, dst_context, dst_context],
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            cp_file_virtual_path = fileset_virtual_location + "/test_cp_file_1.par"
            fs.cp_file(file_virtual_path, cp_file_virtual_path)
            self.assertTrue(fs.exists(cp_file_virtual_path))
            with local_fs.open(sub_file_path, "rb") as f:
                result = f.read()
            self.assertEqual(b"test_file_1", result)

        # test invalid dst path
        cp_file_invalid_virtual_path = (
            "fileset/fileset_catalog/tmp/invalid_fileset/test_cp_file_1.par"
        )
        with self.assertRaises(GravitinoRuntimeException):
            fs.cp_file(file_virtual_path, cp_file_invalid_virtual_path)

    def test_mv(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_mv"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_mv"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        another_dir_path = f"{fileset_storage_location}/another_dir"
        local_fs.mkdirs(another_dir_path)
        self.assertTrue(local_fs.exists(another_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=mock_base.mock_get_fileset_context(
                "test_mv", f"{self._fileset_dir}/test_mv", actual_path
            ),
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path = fileset_storage_location + "/test_file_1.par"
        src_context = mock_base.mock_get_fileset_context(
            "test_mv", f"{self._fileset_dir}/test_mv", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=src_context,
        ):
            self.assertTrue(fs.exists(file_virtual_path))

        mv_file_virtual_path = fileset_virtual_location + "/test_cp_file_1.par"
        actual_path1 = fileset_storage_location + "/test_cp_file_1.par"
        dst_context = mock_base.mock_get_fileset_context(
            "test_mv", f"{self._fileset_dir}/test_mv", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            side_effect=[src_context, dst_context, dst_context],
        ):
            fs.mv(file_virtual_path, mv_file_virtual_path)
            self.assertTrue(fs.exists(mv_file_virtual_path))

        mv_another_dir_virtual_path = (
            fileset_virtual_location + "/another_dir/test_file_2.par"
        )

        actual_path2 = fileset_storage_location + "/another_dir/test_file_2.par"
        dst_context1 = mock_base.mock_get_fileset_context(
            "test_mv", f"{self._fileset_dir}/test_mv", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            side_effect=[dst_context, dst_context1, dst_context1],
        ):
            fs.mv(mv_file_virtual_path, mv_another_dir_virtual_path)
            self.assertTrue(fs.exists(mv_another_dir_virtual_path))

        # test not exist dir
        not_exist_dst_dir_path = fileset_virtual_location + "/not_exist/test_file_2.par"
        actual_path3 = fileset_storage_location + "/not_exist/test_file_2.par"
        dst_context2 = mock_base.mock_get_fileset_context(
            "test_mv", f"{self._fileset_dir}/test_mv", actual_path3
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            side_effect=[dst_context1, dst_context2],
        ):
            with self.assertRaises(FileNotFoundError):
                fs.mv(path1=mv_another_dir_virtual_path, path2=not_exist_dst_dir_path)

        # test invalid dst path
        mv_file_invalid_virtual_path = (
            "fileset/fileset_catalog/tmp/invalid_fileset/test_cp_file_1.par"
        )
        with self.assertRaises(GravitinoRuntimeException):
            fs.mv(path1=file_virtual_path, path2=mv_file_invalid_virtual_path)

    def test_rm(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_rm"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_rm"
        actual_path = fileset_storage_location
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_rm", f"{self._fileset_dir}/test_rm", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test delete file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        context2 = mock_base.mock_get_fileset_context(
            "test_rm", f"{self._fileset_dir}/test_rm", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            fs.rm(file_virtual_path)
            self.assertFalse(fs.exists(file_virtual_path))

        # test delete dir with recursive = false
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_rm", f"{self._fileset_dir}/test_rm", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises(ValueError):
                fs.rm(dir_virtual_path, recursive=False)

            # test delete dir with recursive = true
            fs.rm(dir_virtual_path, recursive=True)
            self.assertFalse(fs.exists(dir_virtual_path))

    def test_rm_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_rm_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_rm_file"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_rm_file", f"{self._fileset_dir}/test_rm_file", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test delete file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        context2 = mock_base.mock_get_fileset_context(
            "test_rm_file", f"{self._fileset_dir}/test_rm_file", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            fs.rm_file(file_virtual_path)
            self.assertFalse(fs.exists(file_virtual_path))

        # test delete dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_rm_file", f"{self._fileset_dir}/test_rm_file", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises((IsADirectoryError, PermissionError)):
                fs.rm_file(dir_virtual_path)

    def test_rmdir(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_rmdir"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_rmdir"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_rmdir", f"{self._fileset_dir}/test_rmdir", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test delete file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        context2 = mock_base.mock_get_fileset_context(
            "test_rmdir", f"{self._fileset_dir}/test_rmdir", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with self.assertRaises(NotADirectoryError):
                fs.rmdir(file_virtual_path)

        # test delete dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_rmdir", f"{self._fileset_dir}/test_rmdir", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            fs.rmdir(dir_virtual_path)
            self.assertFalse(fs.exists(dir_virtual_path))

    def test_open(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_open"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_open"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_open", f"{self._fileset_dir}/test_open", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test open and write file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        context2 = mock_base.mock_get_fileset_context(
            "test_open", f"{self._fileset_dir}/test_open", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with fs.open(file_virtual_path, mode="wb") as f:
                f.write(b"test_open_write")
            self.assertTrue(fs.info(file_virtual_path)["size"] > 0)

            # test open and read file
            with fs.open(file_virtual_path, mode="rb") as f:
                self.assertEqual(b"test_open_write", f.read())

        # test open dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_open", f"{self._fileset_dir}/test_open", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises(IsADirectoryError):
                fs.open(dir_virtual_path)

    def test_mkdir(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_mkdir"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_mkdir"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_mkdir", f"{self._fileset_dir}/test_mkdir", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

            # test mkdir dir which exists
            existed_dir_virtual_path = fileset_virtual_location
            self.assertTrue(fs.exists(existed_dir_virtual_path))
            with self.assertRaises(FileExistsError):
                fs.mkdir(existed_dir_virtual_path)

        # test mkdir dir with create_parents = false
        parent_not_exist_virtual_path = fileset_virtual_location + "/not_exist/sub_dir"
        actual_path1 = fileset_storage_location + "/not_exist/sub_dir"
        context2 = mock_base.mock_get_fileset_context(
            "test_mkdir", f"{self._fileset_dir}/test_mkdir", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertFalse(fs.exists(parent_not_exist_virtual_path))
            with self.assertRaises(FileNotFoundError):
                fs.mkdir(parent_not_exist_virtual_path, create_parents=False)

        # test mkdir dir with create_parents = true
        parent_not_exist_virtual_path2 = fileset_virtual_location + "/not_exist/sub_dir"
        actual_path2 = fileset_storage_location + "/not_exist/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_mkdir", f"{self._fileset_dir}/test_mkdir", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            self.assertFalse(fs.exists(parent_not_exist_virtual_path2))
            fs.mkdir(parent_not_exist_virtual_path2, create_parents=True)
            self.assertTrue(fs.exists(parent_not_exist_virtual_path2))

    def test_makedirs(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_makedirs"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_makedirs"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_makedirs", f"{self._fileset_dir}/test_makedirs", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

            # test mkdir dir which exists
            existed_dir_virtual_path = fileset_virtual_location
            self.assertTrue(fs.exists(existed_dir_virtual_path))
            with self.assertRaises(FileExistsError):
                fs.mkdirs(existed_dir_virtual_path)

        # test mkdir dir not exist
        parent_not_exist_virtual_path = fileset_virtual_location + "/not_exist/sub_dir"
        actual_path1 = fileset_storage_location + "/not_exist/sub_dir"
        context2 = mock_base.mock_get_fileset_context(
            "test_makedirs", f"{self._fileset_dir}/test_makedirs", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertFalse(fs.exists(parent_not_exist_virtual_path))
            fs.makedirs(parent_not_exist_virtual_path)
            self.assertTrue(fs.exists(parent_not_exist_virtual_path))

    def test_created(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_created"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_created"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_created", f"{self._fileset_dir}/test_created", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test mkdir dir which exists
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path1 = fileset_storage_location + "/sub_dir"
        context2 = mock_base.mock_get_fileset_context(
            "test_created", f"{self._fileset_dir}/test_created", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            self.assertIsNotNone(fs.created(dir_virtual_path))

    def test_modified(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_modified"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_modified"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_modified", f"{self._fileset_dir}/test_modified", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test mkdir dir which exists
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path1 = fileset_storage_location + "/sub_dir"
        context2 = mock_base.mock_get_fileset_context(
            "test_modified", f"{self._fileset_dir}/test_modified", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            self.assertIsNotNone(fs.modified(dir_virtual_path))

    def test_cat_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_cat_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_cat_file"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_cat_file", f"{self._fileset_dir}/test_cat_file", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test open and write file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        context2 = mock_base.mock_get_fileset_context(
            "test_cat_file", f"{self._fileset_dir}/test_cat_file", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with fs.open(file_virtual_path, mode="wb") as f:
                f.write(b"test_cat_file")
            self.assertTrue(fs.info(file_virtual_path)["size"] > 0)

            # test cat file
            content = fs.cat_file(file_virtual_path)
            self.assertEqual(b"test_cat_file", content)

        # test cat dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_cat_file", f"{self._fileset_dir}/test_cat_file", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            self.assertTrue(fs.exists(dir_virtual_path))
            with self.assertRaises(IsADirectoryError):
                fs.cat_file(dir_virtual_path)

    def test_get_file(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_get_file"
        fileset_virtual_location = "fileset/fileset_catalog/tmp/test_get_file"
        actual_path = fileset_storage_location

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        sub_file_path = f"{fileset_storage_location}/test_file_1.par"
        local_fs.touch(sub_file_path)
        self.assertTrue(local_fs.exists(sub_file_path))

        sub_dir_path = f"{fileset_storage_location}/sub_dir"
        local_fs.mkdirs(sub_dir_path)
        self.assertTrue(local_fs.exists(sub_dir_path))

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )

        context1 = mock_base.mock_get_fileset_context(
            "test_get_file", f"{self._fileset_dir}/test_get_file", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            self.assertTrue(fs.exists(fileset_virtual_location))

        # test open and write file
        file_virtual_path = fileset_virtual_location + "/test_file_1.par"
        actual_path1 = fileset_storage_location + "/test_file_1.par"
        context2 = mock_base.mock_get_fileset_context(
            "test_get_file", f"{self._fileset_dir}/test_get_file", actual_path1
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context2,
        ):
            self.assertTrue(fs.exists(file_virtual_path))
            with fs.open(file_virtual_path, mode="wb") as f:
                f.write(b"test_get_file")
            self.assertTrue(fs.info(file_virtual_path)["size"] > 0)

            # test get file
            local_path = self._fileset_dir + "/local_file.par"
            local_fs.touch(local_path)
            self.assertTrue(local_fs.exists(local_path))
            fs.get_file(file_virtual_path, local_path)
            self.assertEqual(b"test_get_file", local_fs.cat_file(local_path))

        # test get a dir
        dir_virtual_path = fileset_virtual_location + "/sub_dir"
        actual_path2 = fileset_storage_location + "/sub_dir"
        context3 = mock_base.mock_get_fileset_context(
            "test_get_file", f"{self._fileset_dir}/test_get_file", actual_path2
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context3,
        ):
            local_path = self._fileset_dir + "/local_dir"
            self.assertTrue(fs.exists(dir_virtual_path))
            fs.get_file(dir_virtual_path, local_path)
            self.assertTrue(local_fs.exists(local_path))

        # test get a file to a remote file
        remote_path = "gvfs://" + fileset_virtual_location + "/test_file_2.par"
        with self.assertRaises(GravitinoRuntimeException):
            fs.get_file(file_virtual_path, remote_path)

    def test_convert_actual_path(self, *mock_methods):
        # test convert actual hdfs path
        audit_dto = AuditDTO(
            _creator="test",
            _create_time="2022-01-01T00:00:00Z",
            _last_modifier="test",
            _last_modified_time="2024-04-05T10:10:35.218Z",
        )
        hdfs_fileset: FilesetDTO = FilesetDTO(
            _name="test_f1",
            _comment="",
            _type=FilesetDTO.Type.MANAGED,
            _storage_location="hdfs://localhost:8090/fileset/test_f1",
            _audit=audit_dto,
            _properties={},
        )

        mock_context: FilesetContextDTO = FilesetContextDTO(
            _fileset=hdfs_fileset,
            _actual_path=f"{hdfs_fileset.storage_location()}/actual_path",
        )
        mock_hdfs_context: FilesetContextPair = FilesetContextPair(
            mock_context, LocalFileSystem()
        )

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        # test actual path not start with storage location
        actual_path = "/not_start_with_storage/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            fs._convert_actual_path(
                actual_path,
                mock_hdfs_context,
                StorageType.HDFS,
                NameIdentifier.of(
                    "metalake_demo", "test_catalog", "test_schema", hdfs_fileset.name()
                ),
            )

        # test actual path start with storage location
        actual_path = "/fileset/test_f1/actual_path"
        virtual_path = fs._convert_actual_path(
            actual_path,
            mock_hdfs_context,
            StorageType.HDFS,
            NameIdentifier.of(
                "metalake_demo", "test_catalog", "test_schema", hdfs_fileset.name()
            ),
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", virtual_path
        )

        # test convert actual local path
        audit_dto = AuditDTO(
            _creator="test",
            _create_time="2022-01-01T00:00:00Z",
            _last_modifier="test",
            _last_modified_time="2024-04-05T10:10:35.218Z",
        )
        local_fileset: FilesetDTO = FilesetDTO(
            _name="test_f1",
            _comment="",
            _type=FilesetDTO.Type.MANAGED,
            _storage_location="file:/tmp/fileset/test_f1",
            _audit=audit_dto,
            _properties={},
        )

        mock_local_context: FilesetContextDTO = FilesetContextDTO(
            _fileset=local_fileset,
            _actual_path=f"{local_fileset.storage_location()}/actual_path",
        )
        mock_local_pair: FilesetContextPair = FilesetContextPair(
            mock_local_context, LocalFileSystem()
        )

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        # test actual path not start with storage location
        actual_path = "/not_start_with_storage/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            fs._convert_actual_path(
                actual_path,
                mock_local_pair,
                StorageType.LOCAL,
                NameIdentifier.of(
                    "metalake_demo", "test_catalog", "test_schema", local_fileset.name()
                ),
            )

        # test actual path start with storage location
        actual_path = "/tmp/fileset/test_f1/actual_path"
        virtual_path = fs._convert_actual_path(
            actual_path,
            mock_local_pair,
            StorageType.LOCAL,
            NameIdentifier.of(
                "metalake_demo", "test_catalog", "test_schema", local_fileset.name()
            ),
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", virtual_path
        )

    def test_convert_info(self, *mock_methods):
        # test convert actual hdfs path
        audit_dto = AuditDTO(
            _creator="test",
            _create_time="2022-01-01T00:00:00Z",
            _last_modifier="test",
            _last_modified_time="2024-04-05T10:10:35.218Z",
        )
        hdfs_fileset: FilesetDTO = FilesetDTO(
            _name="test_f1",
            _comment="",
            _type=FilesetDTO.Type.MANAGED,
            _storage_location="hdfs://localhost:8090/fileset/test_f1",
            _audit=audit_dto,
            _properties={},
        )

        mock_hdfs_context: FilesetContextDTO = FilesetContextDTO(
            _fileset=hdfs_fileset,
            _actual_path=f"{hdfs_fileset.storage_location()}/actual_path",
        )
        mock_hdfs_pair: FilesetContextPair = FilesetContextPair(
            mock_hdfs_context, LocalFileSystem()
        )

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        # test actual path not start with storage location
        actual_path = "/not_start_with_storage/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            fs._convert_actual_path(
                actual_path,
                mock_hdfs_pair,
                StorageType.HDFS,
                NameIdentifier.of(
                    "metalake_demo", "test_catalog", "test_schema", hdfs_fileset.name()
                ),
            )

        # test actual path start with storage location
        actual_path = "/fileset/test_f1/actual_path"
        virtual_path = fs._convert_actual_path(
            actual_path,
            mock_hdfs_pair,
            StorageType.HDFS,
            NameIdentifier.of(
                "metalake_demo", "test_catalog", "test_schema", hdfs_fileset.name()
            ),
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", virtual_path
        )

        # test convert actual local path
        audit_dto = AuditDTO(
            _creator="test",
            _create_time="2022-01-01T00:00:00Z",
            _last_modifier="test",
            _last_modified_time="2024-04-05T10:10:35.218Z",
        )
        local_fileset: FilesetDTO = FilesetDTO(
            _name="test_f1",
            _comment="",
            _type=FilesetDTO.Type.MANAGED,
            _storage_location="file:/tmp/fileset/test_f1",
            _audit=audit_dto,
            _properties={},
        )

        mock_local_context: FilesetContextDTO = FilesetContextDTO(
            _fileset=local_fileset,
            _actual_path=f"{local_fileset.storage_location()}/actual_path",
        )
        mock_local_pair: FilesetContextPair = FilesetContextPair(
            mock_local_context, LocalFileSystem()
        )

        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        # test actual path not start with storage location
        actual_path = "/not_start_with_storage/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            fs._convert_actual_path(
                actual_path,
                mock_local_pair,
                StorageType.LOCAL,
                NameIdentifier.of(
                    "metalake_demo", "test_catalog", "test_schema", local_fileset.name()
                ),
            )

        # test actual path start with storage location
        actual_path = "/tmp/fileset/test_f1/actual_path"
        virtual_path = fs._convert_actual_path(
            actual_path,
            mock_local_pair,
            StorageType.LOCAL,
            NameIdentifier.of(
                "metalake_demo", "test_catalog", "test_schema", local_fileset.name()
            ),
        )
        self.assertEqual(
            "fileset/test_catalog/test_schema/test_f1/actual_path", virtual_path
        )

    def test_extract_identifier(self, *mock_methods):
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090", metalake_name="metalake_demo"
        )
        with self.assertRaises(GravitinoRuntimeException):
            fs._extract_identifier(path=None)

        invalid_path = "s3://bucket_1/test_catalog/schema/fileset/ttt"
        with self.assertRaises(GravitinoRuntimeException):
            fs._extract_identifier(path=invalid_path)

        valid_path = "fileset/test_catalog/schema/fileset/ttt"
        identifier: NameIdentifier = fs._extract_identifier(path=valid_path)
        self.assertEqual("metalake_demo", identifier.namespace().level(0))
        self.assertEqual("test_catalog", identifier.namespace().level(1))
        self.assertEqual("schema", identifier.namespace().level(2))
        self.assertEqual("fileset", identifier.name())

    def test_pandas(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_pandas"
        fileset_virtual_location = "gvfs://fileset/fileset_catalog/tmp/test_pandas"

        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)

        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090", metalake_name="test_metalake"
        )

        actual_path = fileset_storage_location + "/test.parquet"
        context1 = mock_base.mock_get_fileset_context(
            "test_pandas", f"{self._fileset_dir}/test_pandas", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            # to parquet
            data.to_parquet(fileset_virtual_location + "/test.parquet", filesystem=fs)
            self.assertTrue(local_fs.exists(fileset_storage_location + "/test.parquet"))

            # read parquet
            ds1 = pandas.read_parquet(
                path=fileset_virtual_location + "/test.parquet", filesystem=fs
            )
            self.assertTrue(data.equals(ds1))

        storage_options = {
            "server_uri": "http://localhost:8090",
            "metalake_name": "test_metalake",
        }

        actual_path1 = fileset_storage_location
        context2 = mock_base.mock_get_fileset_context(
            "test_pandas", f"{self._fileset_dir}/test_pandas", actual_path1
        )

        actual_path2 = fileset_storage_location + "/test.csv"
        context3 = mock_base.mock_get_fileset_context(
            "test_pandas", f"{self._fileset_dir}/test_pandas", actual_path2
        )

        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            side_effect=[context2, context3, context3],
        ):
            # to csv
            data.to_csv(
                fileset_virtual_location + "/test.csv",
                index=False,
                storage_options=storage_options,
            )
            self.assertTrue(local_fs.exists(fileset_storage_location + "/test.csv"))

            # read csv
            ds2 = pandas.read_csv(
                fileset_virtual_location + "/test.csv", storage_options=storage_options
            )
            self.assertTrue(data.equals(ds2))

    def test_pyarrow(self, *mock_methods):
        fileset_storage_location = f"{self._fileset_dir}/test_pyarrow"
        fileset_virtual_location = "gvfs://fileset/fileset_catalog/tmp/test_pyarrow"
        local_fs = LocalFileSystem()
        local_fs.mkdir(fileset_storage_location)
        data = pandas.DataFrame({"Name": ["A", "B", "C", "D"], "ID": [20, 21, 19, 18]})
        fs = gvfs.GravitinoVirtualFileSystem(
            server_uri="http://localhost:8090", metalake_name="test_metalake"
        )
        actual_path = fileset_storage_location + "/test.parquet"
        context1 = mock_base.mock_get_fileset_context(
            "test_pyarrow", f"{self._fileset_dir}/test_pyarrow", actual_path
        )
        with patch(
            "gravitino.catalog.fileset_catalog.FilesetCatalog.get_fileset_context",
            return_value=context1,
        ):
            # to parquet
            data.to_parquet(fileset_virtual_location + "/test.parquet", filesystem=fs)
            self.assertTrue(local_fs.exists(fileset_storage_location + "/test.parquet"))

            # read as arrow dataset
            arrow_dataset = dt.dataset(
                fileset_virtual_location + "/test.parquet", filesystem=fs
            )
            arrow_tb_1 = arrow_dataset.to_table()
            arrow_tb_2 = pa.Table.from_pandas(data)
            self.assertTrue(arrow_tb_1.equals(arrow_tb_2))

            # read as arrow parquet dataset
            arrow_tb_3 = pq.read_table(
                fileset_virtual_location + "/test.parquet", filesystem=fs
            )
            self.assertTrue(arrow_tb_3.equals(arrow_tb_2))
