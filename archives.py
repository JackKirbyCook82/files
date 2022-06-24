# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   Archive Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from abc import ABC
from io import BytesIO
from zipfile import ZipFile

from utilities.meta import RegistryMeta, LockingMeta
from utilities.dispatchers import keyword_single_dispatcher as dispatcher

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Archive"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


class ArchiveBase(object):
    def __init__(self, *args, directory, **kwargs):
        self.__directory = directory
        self.__archive = None
        self.__mode = None
        self.__handler = None

    def __repr__(self): return "{}(directory={})".format(self.__class__.__name__, self.directory)
    def __str__(self): return str(self.directory)
    def __bool__(self): return self.mode is not None

    def __enter__(self): return self
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    def __call__(self, *args, **kwargs):
        if self.handler is None:
            self.handler = self.execute(*args, **kwargs)
        return self.handler

    @property
    def directory(self): return self.__directory
    @property
    def archive(self): return self.__archive
    @archive.setter
    def archive(self, archive): self.__archive = archive
    @property
    def handler(self): return self.__handler
    @handler.setter
    def handler(self, handler): self.__handler = handler
    @property
    def mode(self): self.__mode
    @mode.setter
    def mode(self, mode): self.__mode = mode

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w"):
            raise ValueError(mode)
        self.archive = ZipFile.open(self.directory, mode="r") if mode == "r" else BytesIO()
        self.mode = mode

    def execute(self, *args, **kwargs):
        return ArchiveHandler[self.mode](self.archive, *args, mode=self.mode, **kwargs)

    def close(self, *args, **kwargs):
        if self.mode == "w":
            archive = open(self.directory, "wb")
            archive.write(self.archive.getbuffer())
            archive.close()
        self.archive.close()
        self.archive = None
        self.mode = None

    def copy(self, archive, exclude=[]):
        reader = ArchiveReader(archive)
        writer = ArchiveWriter(self)
        for file in iter(reader):
            if file not in _filter(exclude):
                writer[file] = reader[file]


class Archive(ArchiveBase, metaclass=LockingMeta):
    def __new__(cls, *args, directory, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(directory):
            raise FileNotFoundError(str(directory))
        elif mode == "x" and os.path.exist(directory):
            raise FileExistsError(str(directory))
        cls.lock(directory)
        instance = super().__new__(cls)
        instance.open(*args, mode=mode, **kwargs)
        return instance

    @dispatcher("mode")
    def open(self, *args, mode, **kwargs): raise KeyError(mode)
    @open.register("r")
    def open_reader(self, *args, mode, **kwargs): super().open(*args, mode="r", **kwargs)
    @open.register("w", "x")
    def open_writer(self, *args, mode, **kwargs): super().open(*args, mode="w", **kwargs)

    @open.register("a")
    def open_appender(self, *args, mode, **kwargs):
        archive = Archive(*args, directory=self.directory, **kwargs)
        archive.open(*args, mode="r", **kwargs)
        super().open(*args, mode="w", **kwargs)
        self.copy(archive, exclude=[])
        archive.close(*args, **kwargs)

    def close(self, *args, **kwargs):
        super().close(*args, **kwargs)
        self.unlock(self.directory)


class FileArchive(ArchiveBase, metaclass=LockingMeta):
    def __new__(cls, *args, directory, file, mode, **kwargs):
        if not os.path.exist(directory):
            raise FileNotFoundError(str(directory))
        cls.lock(directory)
        instance = super().__new__(cls)
        instance.open(*args, mode=mode, **kwargs)
        return instance

    @dispatcher("mode")
    def open(self, *args, mode, **kwargs): raise KeyError(mode)
    @open.register("r")
    def open_reader(self, *args, mode, **kwargs): pass
    @open.register("w")
    def open_writer(self, *args, mode, **kwargs): pass
    @open.register("x")
    def open_creator(self, *args, mode, **kwargs): pass
    @open.register("a")
    def open_appender(self, *args, mode, **kwargs): pass

    def close(self, *args, **kwargs):
        super().close(*args, **kwargs)
        self.unlock(self.directory)


class ArchiveHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, archive, *args, **kwargs): self.__archive = archive
    def __iter__(self): return (file for file in self.archive.namelist())
    def __contains__(self, file): return file in self.archive.namelist()

    @property
    def archive(self): return self.__archive


class ArchiveReader(ArchiveHandler, key="r"):
    def __getitem__(self, file): return self.archive.read(file)


class ArchiveWriter(ArchiveHandler, keys="w"):
    def __setitem__(self, file, content): self.archive.writestr(file, content)




