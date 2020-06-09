# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
FileSystem abstraction to interact with various local and remote filesystems.
"""

from pyarrow._fs import (  # noqa
    FileSelector,
    FileType,
    FileInfo,
    FileSystem,
    LocalFileSystem,
    SubTreeFileSystem,
    _MockFileSystem,
    _normalize_path,
    FileSystemHandler,
    PyFileSystem,
)

# For backward compatibility.
FileStats = FileInfo

try:
    from pyarrow._hdfs import HadoopFileSystem  # noqa
except ImportError:
    pass

try:
    from pyarrow._s3fs import S3FileSystem, initialize_s3, finalize_s3  # noqa
except ImportError:
    pass
else:
    initialize_s3()


class FSSpecHandler(FileSystemHandler):
    """
    Handler for fsspec-based Python filesystems.

    https://filesystem-spec.readthedocs.io/en/latest/index.html

    >>> PyFileSystem(FSSpecHandler(fsspec_fs))
    """
    def __init__(self, fs):
        self.fs = fs

    def __eq__(self, other):
        if isinstance(other, FSSpecHandler):
            return self.fs == other.fs
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, FSSpecHandler):
            return self.fs != other.fs
        return NotImplemented

    def get_type_name(self):
        return "fsspec+{0}".format(self.fs.protocol)

    def get_file_info(self, paths):
        infos = []
        for path in paths:
            try:
                info = self.fs.info(path)
            except FileNotFoundError:
                infos.append(FileInfo(path, FileType.NotFound))
            else:
                if info["type"] == "file":
                    ftype = FileType.File
                elif info["type"] == "directory":
                    ftype = FileType.Directory
                else:
                    ftype = FileType.Unknown
                infos.append(FileInfo(
                    path, ftype, size=info["size"],
                    mtime=info.get("mtime", None)
                ))
        return infos

    def get_file_info_selector(self, selector):
        if not self.fs.isdir(selector.base_dir):
            if self.fs.exists(selector.base_dir):
                raise NotADirectoryError(selector.base_dir)
            else:
                if selector.allow_not_found:
                    return []
                else:
                    raise FileNotFoundError(selector.base_dir)

        if selector.recursive:
            maxdepth = None
        else:
            maxdepth = 1

        infos = []
        for path, info in self.fs.find(
            selector.base_dir, maxdepth=maxdepth, withdirs=True, detail=True
        ).items():
            if info["type"] == "file":
                ftype = FileType.File
            elif info["type"] == "directory":
                ftype = FileType.Directory
            else:
                ftype = FileType.Unknown
            infos.append(
                FileInfo(
                    path, ftype, size=info["size"],
                    mtime=info.get("mtime", None)
                )
            )

        return infos

    def create_dir(self, path, recursive):
        # mkdir also raises FileNotFoundError when base directory is not found
        self.fs.mkdir(path, create_parents=recursive)

    def delete_dir(self, path):
        self.fs.rm(path, recursive=True)

    def delete_dir_contents(self, path):
        # TODO
        raise NotImplementedError

    def delete_file(self, path):
        # TODO do we need to check here that `path` actually is a file?
        # or is this already checked before calling this?
        self.fs.rm(path)

    def move(self, src, dest):
        # TODO
        raise NotImplementedError

    def copy_file(self, src, dest):
        # TODO
        raise NotImplementedError

    def open_input_stream(self, path):
        from pyarrow import PythonFile

        if not self.fs.isfile(path):
            raise FileNotFoundError(path)
        # with self.fs.open(path) as f:
        #     return pa.PythonFile(f)
        # with self.fs.open(path) as f:
        #     buf = f.read()
        # return pa.BufferReader(buf)
        # return pa.PythonFile(self.fs.open(path).f)
        return PythonFile(self.fs.open(path))

    def open_input_file(self, path):
        from pyarrow import PythonFile

        if not self.fs.isfile(path):
            raise FileNotFoundError(path)
        # with self.fs.open(path) as f:
        #     buf = f.read()
        # return pa.BufferReader(buf)
        # TODO need to find a robust way to get a file-like object from fsspec
        return PythonFile(self.fs.open(path))

    def open_output_stream(self, path):
        if not self.fs.isfile(path):
            raise FileNotFoundError(path)
        # TODO
        raise NotImplementedError
        # return pa.BufferOutputStream()

    def open_append_stream(self, path):
        if not self.fs.isfile(path):
            raise FileNotFoundError(path)
        # TODO
        raise NotImplementedError
        # return pa.BufferOutputStream()
