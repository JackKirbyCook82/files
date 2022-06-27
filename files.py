# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   File Reader/Writer Objects
@author: Jack Kirby Cook

"""

from enum import Enum
from abc import ABC, ABCMeta, abstractmethod

from utilities.meta import RegistryMeta, LockingMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["FileMeta", "FileBase", "File", "FileLocation"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]


class FileLocation(Enum):
    START = 0
    CURRENT = 1
    STOP = 2


class FileBase(ABC):
    def __init__(self, *args, **kwargs):
        self.__mode = None
        self.__handler = None

    def __bool__(self): return self.mode is not None
    def __enter__(self): return self
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    def __call__(self, *args, **kwargs):
        if self.handler is None:
            self.handler = self.execute(*args, **kwargs)
        return self.handler

    @property
    def handler(self): return self.__handler
    @handler.setter
    def handler(self, handler): self.__handler = handler
    @property
    def mode(self): self.__mode
    @mode.setter
    def mode(self, mode): self.__mode = mode
    @property
    def readable(self): return self.mode == "r"
    @property
    def writeable(self): return self.mode in ("w", "x", "a")

    @abstractmethod
    def open(self, *args, mode, **kwargs): pass
    @abstractmethod
    def execute(self, *args, **kwargs): pass
    @abstractmethod
    def close(self, *args, **kwargs): pass


class FileMeta(LockingMeta, ABCMeta):
    def __call__(cls, *args, file, **kwargs):
        instance = super(FileMeta, cls).__call__(*args, **kwargs)
        instance.lock(str(instance))
        instance.open(*args, **kwargs)
        return instance


class File(FileBase, metaclass=FileMeta):
    def __init__(self, *args, file, **kwargs):
        self.__file = file
        self.__source = None
        super().__init__(*args, **kwargs)

    def __repr__(self): return "{}(file={})".format(self.__class__.__name__, self.file)
    def __str__(self): return str(self.file)

    @property
    def file(self): return self.__file
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        self.source = open(self.file, mode=mode)
        self.mode = mode

    def execute(self, *args, **kwargs):
        return FileHandler[self.mode](self.source, *args, **kwargs)

    def close(self, *args, **kwargs):
        self.source.close()
        self.source = None
        self.handler = None
        self.mode = None
        self.unlock(str(self))


class FileHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, **kwargs):
        self.__source = source

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


