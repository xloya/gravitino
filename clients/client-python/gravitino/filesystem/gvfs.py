"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import errno
import os
import secrets
import shutil
import fsspec
import io
import regex

from contextlib import suppress
from enum import Enum
from fsspec import utils
from gravitino.api.catalog import Catalog
from gravitino.api.fileset import Fileset
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.name_identifier import NameIdentifier
from pyarrow.fs import FileSystem
from readerwriterlock import rwlock
from typing import Dict, Tuple

PROTOCOL_NAME = "gvfs"


class StorageType(Enum):
    HDFS = "hdfs://"


class FilesetContext:
    def __init__(
        self,
        name_identifier: NameIdentifier,
        fileset: Fileset,
        fs: FileSystem,
        actual_path,
    ):
        self.name_identifier = name_identifier
        self.fileset = fileset
        self.fs = fs
        self.actual_path = actual_path

    def get_name_identifier(self):
        return self.name_identifier

    def get_fileset(self):
        return self.fileset

    def get_fs(self):
        return self.fs

    def get_actual_path(self):
        return self.actual_path


class GravitinoVirtualFileSystem(fsspec.AbstractFileSystem):
    protocol = PROTOCOL_NAME
    _gvfs_prefix = "gvfs://fileset"
    _identifier_pattern = regex.compile(
        "^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$"
    )

    def __init__(self, server_uri, metalake_name, **kwargs):
        self.metalake = metalake_name
        self.client = GravitinoClient(uri=server_uri, metalake_name=metalake_name)
        self.cache: Dict[NameIdentifier, Tuple] = {}
        self.cache_lock = rwlock.RWLockFair()

        super().__init__(**kwargs)

    def _strip_protocol(cls, path):
        ops = utils.infer_storage_options(path)
        path = ops["path"]
        if path.startswith("//"):
            # special case for "hdfs://path" (without the triple slash)
            path = path[1:]
        return path

    def ls(self, path, detail=True, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            from pyarrow.fs import FileSelector

            entries = []
            for entry in context.fs.get_file_info(
                FileSelector(self._strip_protocol(context.actual_path))
            ):
                entries.append(
                    self._convert_file_info_path_prefix(
                        entry,
                        context.fileset.storage_location(),
                        self._get_virtual_location(context.name_identifier, True),
                    )
                )
            return entries
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def info(self, path, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            [info] = context.fs.get_file_info([actual_path])

            return self._convert_file_info_path_prefix(
                info,
                context.fileset.storage_location(),
                self._get_virtual_location(context.name_identifier, True),
            )
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def exists(self, path, **kwargs):
        try:
            self.info(path)
        except FileNotFoundError:
            return False
        else:
            return True

    def cp_file(self, src, dst, **kwargs):
        src_identifier: NameIdentifier = self._extract_identifier(src)
        dst_identifier: NameIdentifier = self._extract_identifier(dst)
        if not src_identifier == dst_identifier:
            raise ValueError(
                "Destination file path identifier: `{}` should be same with src file path identifier: `{}`.".format(
                    dst_identifier, src_identifier
                )
            )
        src_context: FilesetContext = self._get_fileset_context(src)
        if src_context.actual_path.startswith(StorageType.HDFS.value):
            if self._check_mount_single_file(src_context.fileset, src_context.fs):
                raise ValueError(
                    "Cannot cp file of the fileset: {} which only mounts to a single file.".format(
                        src_identifier
                    )
                )
            dst_context: FilesetContext = self._get_fileset_context(dst)

            src_actual_path = self._strip_protocol(src_context.actual_path).rstrip("/")
            dst_actual_path = self._strip_protocol(dst_context.actual_path).rstrip("/")

            with src_context.fs.open_input_stream(src_actual_path) as lstream:
                tmp_dst_name = f"{dst_actual_path}.tmp.{secrets.token_hex(6)}"
                try:
                    with dst_context.fs.open_output_stream(tmp_dst_name) as rstream:
                        shutil.copyfileobj(lstream, rstream)
                    dst_context.fs.move(tmp_dst_name, dst_actual_path)
                except BaseException:  # noqa
                    with suppress(FileNotFoundError):
                        dst_context.fs.delete_file(tmp_dst_name)
                    raise
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    src_context.name_identifier
                )
            )

    def mv(self, src, dst, **kwargs):
        src_identifier: NameIdentifier = self._extract_identifier(src)
        dst_identifier: NameIdentifier = self._extract_identifier(dst)
        if not src_identifier == dst_identifier:
            raise ValueError(
                "Destination file path identifier: `{}` should be same with src file path identifier: `{}`.".format(
                    dst_identifier, src_identifier
                )
            )
        src_context: FilesetContext = self._get_fileset_context(src)
        if src_context.actual_path.startswith(StorageType.HDFS.value):
            if self._check_mount_single_file(src_context.fileset, src_context.fs):
                raise ValueError(
                    "Cannot cp file of the fileset: {} which only mounts to a single file.".format(
                        src_identifier
                    )
                )
            dst_context: FilesetContext = self._get_fileset_context(dst)

            src_actual_path = self._strip_protocol(src_context.actual_path).rstrip("/")
            dst_actual_path = self._strip_protocol(dst_context.actual_path).rstrip("/")

            src_context.fs.move(src_actual_path, dst_actual_path)
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    src_context.name_identifier
                )
            )

    def rm_file(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            context.fs.delete_file(actual_path)
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def rm(self, path, recursive=False, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            from pyarrow.fs import FileType

            actual_path = self._strip_protocol(context.actual_path).rstrip("/")
            [info] = context.fs.get_file_info([actual_path])
            if info.type is FileType.Directory:
                if recursive:
                    context.fs.delete_dir(actual_path)
                else:
                    raise ValueError("Cannot delete directories that recursive is `False`.")
            else:
                context.fs.delete_file(actual_path)
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def _open(self, path, mode="rb", block_size=None, seekable=True, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            if mode == "rb":
                if seekable:
                    method = context.fs.open_input_file
                else:
                    method = context.fs.open_input_stream
            elif mode == "wb":
                method = context.fs.open_output_stream
            elif mode == "ab":
                method = context.fs.open_append_stream
            else:
                raise ValueError("Unsupported mode for Arrow FileSystem: {}.".format(mode))

            _kwargs = {}
            if mode != "rb" or not seekable:
                _kwargs["compression"] = None
            stream = method(context.actual_path, **_kwargs)

            return HDFSFile(
                self, stream, context.actual_path, mode, block_size, **kwargs
            )
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def mkdir(self, path, create_parents=True, **kwargs):
        if create_parents:
            self.makedirs(path, exist_ok=True)
        else:
            context: FilesetContext = self._get_fileset_context(path)
            if context.actual_path.startswith(StorageType.HDFS.value):
                actual_path = self._strip_protocol(context.actual_path)
                context.fs.create_dir(actual_path, recursive=False)
            else:
                raise ValueError(
                    "Storage under the fileset: `{}` doesn't support now.".format(
                        context.name_identifier
                    )
                )

    def makedirs(self, path, exist_ok=True):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            context.fs.create_dir(actual_path)
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def rmdir(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            context.fs.delete_dir(actual_path)
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def modified(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            return context.fs.get_file_info(actual_path).mtime
        else:
            raise ValueError(
                "Storage under the fileset: `{}` doesn't support now.".format(
                    context.name_identifier
                )
            )

    def cat_file(self, path, start=None, end=None, **kwargs):
        NotImplementedError()

    def get_file(self, rpath, lpath, **kwargs):
        NotImplementedError()

    def _convert_file_info_path_prefix(self, info, storage_location, virtual_location):
        from pyarrow.fs import FileType

        actual_prefix = self._strip_protocol(storage_location)
        virtual_prefix = self._strip_protocol(virtual_location)
        if not info.path.startswith(actual_prefix):
            raise ValueError(
                "Path {} does not start with valid prefix {}.".format(info.path, actual_prefix)
            )

        if info.type is FileType.Directory:
            kind = "directory"
        elif info.type is FileType.File:
            kind = "file"
        elif info.type is FileType.NotFound:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), info.path)
        else:
            kind = "other"

        return {
            "name": "{}{}".format(
                self._gvfs_prefix, info.path.replace(actual_prefix, virtual_prefix)
            ),
            "size": info.size,
            "type": kind,
            "mtime": info.mtime,
        }

    def _get_fileset_context(self, virtual_path: str) -> FilesetContext:
        identifier: NameIdentifier = self._extract_identifier(virtual_path)
        read_lock = self.cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[Fileset, FileSystem] = self.cache.get(identifier)
            if cache_value is not None:
                actual_path = self._get_actual_path_by_ident(
                    identifier, cache_value[0], cache_value[1], virtual_path
                )
                return FilesetContext(
                    identifier, cache_value[0], cache_value[1], actual_path
                )
        finally:
            read_lock.release()

        write_lock = self.cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[Fileset, FileSystem] = self.cache.get(identifier)
            if cache_value is not None:
                actual_path = self._get_actual_path_by_ident(
                    identifier, cache_value[0], cache_value[1], virtual_path
                )
                return FilesetContext(
                    identifier, cache_value[0], cache_value[1], actual_path
                )
            fileset: Fileset = self._load_fileset_from_server(identifier)
            storage_location = fileset.storage_location()
            if storage_location.startswith(StorageType.HDFS.value):
                from pyarrow.fs import HadoopFileSystem

                fs = HadoopFileSystem.from_uri(storage_location)
                actual_path = self._get_actual_path_by_ident(
                    identifier, fileset, fs, virtual_path
                )
                self.cache[identifier] = (fileset, fs)
                context = FilesetContext(identifier, fileset, fs, actual_path)
                return context
            else:
                raise ValueError(
                    "Storage under the fileset: `{}` doesn't support now.".format(
                        identifier
                    )
                )
        finally:
            write_lock.release()

    def _extract_identifier(self, path) -> NameIdentifier:
        if path is None or len(path) == 0:
            raise ValueError("path which need be extracted cannot be null or empty.")
        match = self._identifier_pattern.match(path)
        if match and len(match.groups()) == 3:
            return NameIdentifier.of_fileset(
                self.metalake,
                match.group(1),
                match.group(2),
                match.group(3),
            )
        else:
            raise ValueError(
                "path: `{}` doesn't contains valid identifier.".format(path)
            )

    def _load_fileset_from_server(self, identifier: NameIdentifier) -> Fileset:
        catalog: Catalog = self.client.load_catalog(
            NameIdentifier.of_catalog(
                identifier.namespace().level(0), identifier.namespace().level(1)
            )
        )
        return catalog.as_fileset_catalog().load_fileset(identifier)

    def _get_actual_path_by_ident(
        self,
        identifier: NameIdentifier,
        fileset: Fileset,
        fs: FileSystem,
        virtual_path: str,
    ) -> str:
        with_scheme = virtual_path.startswith(self._gvfs_prefix)
        virtual_location = self._get_virtual_location(identifier, with_scheme)
        storage_location = fileset.storage_location()
        try:
            if self._check_mount_single_file(fileset, fs):
                if virtual_path != virtual_location:
                    raise Exception(
                        "Path: {} should be same with the virtual location: {} when the fileset only mounts a single file.".format(
                            virtual_path, virtual_location
                        )
                    )
                return storage_location
            else:
                return virtual_path.replace(virtual_location, storage_location, 1)
        except Exception as e:
            raise Exception(
                "Cannot resolve path: {} to actual storage path, exception: {}".format(
                    virtual_path, str(e)
                )
            )

    def _get_virtual_location(
        self, identifier: NameIdentifier, with_scheme: bool
    ) -> str:
        return "{}/{}/{}/{}".format(
            self._gvfs_prefix if with_scheme is True else "",
            identifier.namespace().level(1),
            identifier.namespace().level(2),
            identifier.name(),
        )

    def _check_mount_single_file(self, fileset: Fileset, fs: FileSystem) -> bool:
        from pyarrow.fs import FileType

        try:
            [info] = fs.get_file_info(
                [(self._strip_protocol(fileset.storage_location()))]
            )
            if info.type is FileType.File:
                return True
            else:
                return False
        except Exception as e:
            raise Exception(
                "Cannot check whether the fileset: {} mounts a single file, exception: {}".format(
                    fileset.storage_location(), str(e)
                )
            )


class HDFSFile(io.IOBase):
    def __init__(self, fs, stream, path, mode, block_size=None, **kwargs):
        self.path = path
        self.mode = mode

        self.fs = fs
        self.stream = stream

        self.blocksize = self.block_size = block_size
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.close()


fsspec.register_implementation(PROTOCOL_NAME, GravitinoVirtualFileSystem)
