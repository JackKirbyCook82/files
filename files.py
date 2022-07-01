# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   File Reader/Writer Objects
@author: Jack Kirby Cook

"""

from enum import Enum
from abc import ABC

from utilities.meta import RegistryMeta, LockingMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["File", "FileLocation", "FileHandler"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]


class FileLocation(Enum):
    START = 0
    CURRENT = 1
    STOP = 2


class OpenedFileError(Exception): pass
class ClosedFileError(Exception): pass


class FileMeta(LockingMeta):
    def __call__(cls, *args, mode, **kwargs):
        if mode not in ("r", "w", "x", "a"):
            raise ValueError(mode)
        instance = super(FileMeta, cls).__call__(*args, **kwargs)
        instance.open(*args, mode=mode, **kwargs)
        instance.execute(*args, mode=mode, **kwargs)
        return instance


class File(object, metaclass=FileMeta):
    def __init__(self, *args, file, **kwargs):
        self.__file = file
        self.__source = None
        self.__handler = None

    def __repr__(self): return "{}(file={})".format(self.__class__.__name__, self.file)
    def __str__(self): return str(self.file)
    def __bool__(self): return self.source is not None

    def __enter__(self, *args, **kwargs): return self.handler
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    @property
    def file(self): return self.__file
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source
    @property
    def handler(self): return self.__handler
    @handler.setter
    def handler(self, handler): self.__handler = handler

    def getSource(self, *args, mode, **kwargs): return open(self.file, mode=mode)
    def getHandler(self, *args, mode, **kwargs): return FileHandler[mode](self.source, *args, **kwargs)

    def open(self, *args, mode, **kwargs):
        assert mode in ("r", "w", "x", "a")
        if bool(self):
            raise OpenedFileError(str(self))
        self.lock(str(self))
        self.source = self.getSource(self.file, *args, mode=mode, **kwargs)

    def close(self, *args, **kwargs):
        if not bool(self):
            raise ClosedFileError(str(self))
        self.source.close()
        self.source = None
        self.handler = None
        self.unlock(str(self))

    def execute(self, *args, mode, **kwargs):
        self.handler = self.getHandler(self.source, *args, mode=mode, **kwargs)


class FileHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, **kwargs): self.__source = source
    @property
    def source(self): return self.__source


class FileReader(FileHandler, key="r"):
    def __call__(self): return self.source.read()
    def __next__(self): return self.source.readline()
    def __iter__(self): return self


class FileWriter(FileHandler, keys=("w", "x")):
    def __call__(self, string): self.source.write(string)


class FileAppender(FileWriter, key="a"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source.seek(0, FileLocation.STOP)


