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

from files.files import FileBase

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ArchiveBase", "Archive", "ArchiveHandler"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


class ArchiveBase(FileBase, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__destination = None

    @property
    def destination(self): return self.__destination


class Archive(ArchiveBase, metaclass=LockingMeta):
    def __new__(cls, *args, file, mode, **kwargs):
        pass

    def open(self, *args, mode, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass


class ArchiveHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, **kwargs): self.__source = source
    def __iter__(self): return (file for file in self.source.namelist())
    def __contains__(self, file): return file in self.source.namelist()
    @property
    def source(self): return self.__source


class ArchiveReader(ArchiveHandler, key="r"):
    def __getitem__(self, file): return self.source.read(file)


class ArchiveWriter(ArchiveHandler, keys="w"):
    def __setitem__(self, file, content): self.source.writestr(file, content)


# class ArchiveBase(FileBase):
#     def open(self, *args, mode, **kwargs):
#         if mode not in ("r", "w"):
#             raise ValueError(mode)
#         directory = self.directory if mode == "r" else self.temporary
#         self.archive = ZipFile.open(directory, mode=mode)
#         self.mode = mode
#
#     def execute(self, *args, **kwargs):
#         return ArchiveHandler[self.mode](self.archive, *args, **kwargs)
#
#     def close(self, *args, **kwargs):
#         self.archive.close()
#         self.archive = None
#         self.mode = None
#
#     def copy(self, archive, *args, exclude=[], **kwargs):
#         reader = ArchiveReader(archive, *args, **kwargs)
#         writer = ArchiveWriter(self, *args, **kwargs)
#         for file in iter(reader):
#             if file not in _filter(exclude):
#                 writer[file] = reader[file]


# class Archive(ArchiveBase, metaclass=LockingMeta):
#     def __new__(cls, *args, directory, mode, **kwargs):
#         if mode not in ("r", "w", "a", "x"):
#             raise ValueError(mode)
#         if mode in ("r", "a") and not os.path.exist(directory):
#             raise FileNotFoundError(str(directory))
#         elif mode == "x" and os.path.exist(directory):
#             raise FileExistsError(str(directory))
#         cls.lock(directory)
#         instance = super().__new__(cls)
#         instance.open(*args, mode=mode, **kwargs)
#         return instance
#
#     @dispatcher("mode")
#     def open(self, *args, mode, **kwargs): raise KeyError(mode)
#     @open.register("r")
#     def open_reader(self, *args, mode, **kwargs): super().open(*args, mode="r", **kwargs)
#     @open.register("w", "x")
#     def open_writer(self, *args, mode, **kwargs): super().open(*args, mode="w", **kwargs)
#
#     @open.register("a")
#     def open_appender(self, *args, mode, **kwargs):
#         archive = ArchiveBase(*args, directory=self.directory, **kwargs)
#         archive.open(*args, mode="r", **kwargs)
#         super().open(*args, mode="w", **kwargs)
#         self.copy(archive, *args, exclude=[], **kwargs)
#         archive.close(*args, **kwargs)
#
#     def close(self, *args, **kwargs):
#         super().close(*args, **kwargs)
#         self.unlock(self.directory)




