# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   File Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from io import BytesIO
from zipfile import ZipFile

from utilities.dispatchers import keywordDispatcher as dispatcher

from files.files import FileBase, FileMeta, File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Archive"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]
_archive = lambda file, *a, mode, **kw: ZipFile(file, mode=mode)


class OpenedArchiveError(Exception): pass
class ClosedArchiveError(Exception): pass


class Archive(FileBase, metaclass=FileMeta, function=_archive):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__reader = None
        self.__writer = None

    @property
    def reader(self): return self.__reader
    @reader.setter
    def reader(self, reader): self.__reader = reader
    @property
    def writer(self): return self.__writer
    @writer.setter
    def writer(self, writer): self.__writer = writer

    def open(self, *args, mode, **kwargs):
        if bool(self):
            raise OpenedArchiveError(str(self))
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(self.file):
            raise FileNotFoundError(str(self.file))
        elif mode == "x" and os.path.exist(self.file):
            raise FileExistsError(str(self.file))
        self.reader = self.function(self.file, *args, mode=mode, **kwargs)
        if self.writeable:
            self.writer = ZipFile(BytesIO(), mode="w")
        self.mode = mode

    def close(self, *args, **kwargs):
        if not bool(self):
            raise ClosedArchiveError(str(self))
        self.reader.close()
        self.reader = None
        if self.writeable:
            with ZipFile(self.directory, mode="w") as archive:
                content = self.writer.getbuffer()
                archive.write(content)
            self.writer.close()
            self.writer = None
        self.mode = None
        self.unlock(str(self))

    @dispatcher("mode")
    def source(self, *args, file, mode, **kwargs): pass

    @source.register("r")
    def source_reader(self, *args, file, mode="r", **kwargs):
        assert mode == "r"
        if file not in self.reader.namelist():
            raise FileNotFoundError(str(self.directory), str(self.file))
        return self.reader

    @source.register("w", "x", "a")
    def source_writer(self, *args, file, mode, **kwargs):
        assert mode in ("w", "x", "a")
        if mode == "a" and file not in self.reader.namelist():
            raise FileNotFoundError(str(self.directory), str(file))
        elif mode == "x" and file in self.reader.namelist():
            raise FileExistsError(str(self.directory), str(file))
        if self.mode == "a" and mode == "a":
            self.copy(self.reader, self.writer, exclude=[])
        elif self.mode == "a" and mode != "a":
            self.copy(self.reader, self.writer, exclude=[file])
        return self.writer

    @staticmethod
    def copy(source, destination, exclude=[]):
        assert isinstance(exclude, list)
        for file in source.namelist():
            if file not in exclude:
                content = source.read(file)
                destination.writestr(file, content)

    def execute(self, *args, **kwargs):
        assert "file" in kwargs.keys() and "mode" in kwargs.keys()
        source = self.source(*args, **kwargs)
        function = lambda file, *a, mode, **kw: source.open(file, mode=mode)
        return File(*args, function=function, **kwargs)



