# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   File Reader/Writer Objects
@author: Jack Kirby Cook

"""

from abc import ABC
from enum import Enum

from utilities.meta import RegistryMeta, LockingMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["File", "FileLocation"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]
_source = lambda file, *a, mode, **kw: open(file, mode=mode)


FileLocation = Enum("FileLocation", "START CURRENT STOP", start=0)


class FileMeta(LockingMeta):
    def __init__(cls, *args, **kwargs):
        cls._source = kwargs.get("source", getattr(cls, "source", None))

    def __call__(cls, *args, file, **kwargs):
        assert cls._source is not None
        assert callable(cls._source)
        cls.lock(file)
        instance = super(FileMeta, cls).__call__(*args, file=file, **kwargs)
        instance.open(*args, source=cls._source, **kwargs)
        return instance


class File(object, metaclass=FileMeta, source=_source):
    def __init__(self, *args, file, **kwargs):
        self.__file = file
        self.__source = None
        self.__mode = None
        self.__handler = None

    def __repr__(self): return "{}(file={})".format(self.__class__.__name__, self.file)
    def __str__(self): return str(self.file)
    def __bool__(self): return self.mode is not None

    def __enter__(self): return self
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    def __call__(self, *args, **kwargs):
        if self.handler is None:
            self.handler = self.execute(*args, **kwargs)
        return self.handler

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

    @property
    def mode(self): self.__mode
    @mode.setter
    def mode(self, mode): self.__mode = mode
    @property
    def readable(self): return self.mode == "r"
    @property
    def writeable(self): return self.mode in ("w", "x", "a")

    def open(self, *args, mode, source, **kwargs):
        assert callable(source)
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        self.source = source(self.file, *args, mode=mode, **kwargs)
        self.mode = mode

    def execute(self, *args, **kwargs):
        return FileHandler[self.mode](self.source, *args, **kwargs)

    def close(self, *args, **kwargs):
        self.source.close()
        self.source = None
        self.handler = None
        self.mode = None


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


