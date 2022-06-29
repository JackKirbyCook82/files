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
__all__ = ["FileBase", "FileMeta", "File", "FileLocation"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]
_file = lambda file, *a, mode, **kw: open(file, mode=mode)


class FileLocation(Enum):
    START = 0
    CURRENT = 1
    STOP = 2


class OpenedFileError(Exception): pass
class ClosedFileError(Exception): pass


class FileBase(ABC, metaclass=ABCMeta):
    def __init__(self, *args, file, **kwargs):
        self.__file = file
        self.__mode = None

    def __repr__(self): return "{}(file={})".format(self.__class__.__name__, self.file)
    def __str__(self): return str(self.file)
    def __bool__(self): return self.mode is not None

    def __enter__(self): return self
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    @property
    def file(self): return self.__file
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
    def close(self, *args, **kwargs): pass


class FileMeta(LockingMeta, ABCMeta):
    def __init__(cls, *args, **kwargs):
        cls._function = kwargs.get("function", getattr(cls, "function", None))

    def __call__(cls, *args, **kwargs):
        function = kwargs.get("function", getattr(cls, "function", None))
        instance = super(FileMeta, cls).__call__(*args, function=function, **kwargs)
        instance.lock(str(instance))
        instance.open(*args, **kwargs)
        return instance

    @property
    def function(cls): return cls._function


class File(FileBase, metaclass=FileMeta, function=_file):
    def __init__(self, *args, function, **kwargs):
        assert function is not None and callable(function)
        super().__init__(*args, **kwargs)
        self.__function = function
        self.__source = None
        self.__handler = None

    def __call__(self, *args, **kwargs):
        if self.handler is None:
            self.handler = self.execute(*args, **kwargs)
        return self.handler

    @property
    def function(self): return self.__function
    @function.setter
    def function(self, function): self.__function = function
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source
    @property
    def handler(self): return self.__handler
    @handler.setter
    def handler(self, handler): self.__handler = handler

    def execute(self, *args, **kwargs):
        return FileHandler[self.mode](self.source, *args, **kwargs)

    def open(self, *args, mode, **kwargs):
        if bool(self):
            raise OpenedFileError(str(self))
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        self.source = self.function(self.file, *args, mode=mode, **kwargs)
        self.mode = mode

    def close(self, *args, **kwargs):
        if not bool(self):
            raise ClosedFileError(str(self))
        self.source.close()
        self.source = None
        self.handler = None
        self.mode = None
        self.unlock(str(self))


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


