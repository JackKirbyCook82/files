# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   File Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from io import BytesIO
from zipfile import ZipFile, Path

from utilities.dispatchers import keywordDispatcher as dispatcher

from files.files import FileMeta, FileHandler

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Archive"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]


class OpenedArchiveError(Exception): pass
class ClosedArchiveError(Exception): pass


class ArchiveMeta(FileMeta):
    def __call__(cls, *args, file, directory, mode, **kwargs):
        instance = super(ArchiveMeta, cls).__call__(*args, directory=directory, file=file, **kwargs)
        return instance


class Archive(object, metaclass=FileMeta):
    def __init__(self, *args, directory, file, **kwargs):
        self.__directory = directory
        self.__file = file
        self.__reader = None
        self.__writer = None
        self.__source = None
        self.__handler = None

    def __repr__(self): return "{}(directory={}, file={})".format(self.__class__.__name__, self.directory, self.file)
    def __str__(self): return str(self.path)
    def __bool__(self): return self.source is not None

    def __enter__(self, *args, **kwargs): return self.handler
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    @property
    def directory(self): return self.__directory
    @property
    def file(self): return self.file
    @property
    def path(self): return Path(self.directory, self.file)
    @property
    def reader(self): return self.__reader
    @reader.setter
    def reader(self, reader): self.__reader = reader
    @property
    def writer(self): return self.__writer
    @writer.setter
    def writer(self, writer): self.__writer = writer
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source
    @property
    def handler(self): return self.__handler
    @handler.setter
    def handler(self, handler): self.__handler = handler

    def getReader(self, *args, **kwargs): return ZipFile(self.directory, mode="r")
    def getWriter(self, *args, **kwargs): return ZipFile(BytesIO(), mode="w")
    def getSource(self, *args, mode, **kwargs): return self.reader.open(self.file, mode=mode) if mode == "r" else self.writer.open(self.file, mode=mode)
    def getHandler(self, *args, mode, **kwargs): return FileHandler[mode](self.source, *args, **kwargs)

    @staticmethod
    def copy(reader, writer, exclude=[]):
        assert isinstance(exclude, list)
        for file in reader.namelist():
            if file not in exclude:
                content = reader.read(file)
                writer.writestr(file, content)

    @dispatcher("mode")
    def open(self, *args, mode, **kwargs): raise KeyError(mode)

    @open.register("r")
    def open_reader(self, *args, mode, **kwargs):
        assert mode == "r"
        if bool(self):
            raise OpenedArchiveError(str(self))
        self.__class__.lock(str(self.file))
        self.reader = self.getReader(*args, mode="r", **kwargs)
        self.source = self.getSource(*args, mode="r", **kwargs)

    @open.register("w", "x", "a")
    def open_writer(self, *args, mode, **kwargs):
        assert mode in ("w", "x", "a")
        if bool(self):
            raise OpenedArchiveError(str(self))
        self.__class__.lock(str(self.file))
        self.writer = self.getWriter(*args, mode="w", **kwargs)
        if os.path.exist(self.directory):
            self.reader = self.getReader(*args, mode="r", **kwargs)
            exclude = [self.file] if mode == "a" else []
            self.copy(self.reader, self.writer, exclude=exclude)
        if self.file in self.reader.namelist() and mode == "x":
            raise FileExistsError(str(self))
        self.source = self.getSource(*args, mode=mode, **kwargs)

    def close(self, *args, **kwargs):
        if not bool(self):
            raise ClosedArchiveError(str(self))
        self.source.close()
        self.source = None
        if self.reader is not None:
            self.reader.close()
            self.reader = None
        if self.writer is not None:
            with ZipFile(self.directory, mode="w") as archive:
                content = self.writer.getbuffer()
                archive.write(content)
            self.writer.close()
            self.writer = None
        self.handler = None
        self.__class__.unlock(str(self.file))

    def execute(self, *args, mode, **kwargs):
        self.handler = self.getHandler(*args, mode=mode, **kwargs)

