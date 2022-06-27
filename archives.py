# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   Archive Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from io import BytesIO
from zipfile import ZipFile
from abc import ABC, ABCMeta

from files.files import FileMeta, FileBase

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ArchiveMeta", "ArchiveBase", "Archive"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


class ArchiveBase(FileBase, ABC):
    pass


class ArchiveMeta(FileMeta, ABCMeta):
    def __call__(cls, *args, directory, **kwargs):
        cls.lock(directory)
        instance = super(FileMeta, cls).__call__(*args, **kwargs)
        instance.open(*args, **kwargs)
        cls.copy(instance.source, instance.destination, exclude=[])
        return instance

    @staticmethod
    def copy(source, destination, exclude=[]):
        assert isinstance(exclude, list)
        for file in source.namelist():
            if file not in exclude:
                content = source.read(file)
                destination.writestr(file, content)


class Archive(ArchiveBase, metaclass=ArchiveMeta):
    def __init__(self, *args, directory, **kwargs):
        self.__directory = directory
        self.__source = None
        self.__destination = None
        super().__init__(*args, **kwargs)

    def __repr__(self): return "{}(directory={})".format(self.__class__.__name__, self.directory)
    def __str__(self): return str(self.directory)

    @property
    def directory(self): return self.__directory
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source
    @property
    def destination(self): return self.__destination
    @destination.setter
    def destination(self, destination): self.__destination = destination

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(self.file):
            raise FileNotFoundError(str(self.file))
        elif mode == "x" and os.path.exist(self.file):
            raise FileExistsError(str(self.file))
        self.source = ZipFile(self.directory, mode="r")
        self.destination = ZipFile(BytesIO(), mode="w")

    def execute(self, *args, file, mode, **kwargs):
        pass

    def close(self, *args, **kwargs):
        self.source.close()
        self.source = None
        if self.writeable:
            with ZipFile(self.file, mode="w") as archive:
                content = self.destination.getbuffer()
                archive.write(content)
        self.destination.close()
        self.destination = None
        self.handler = None
        self.mode = None
        self.unlock()


class ArchiveFileBase(FileBase, ABC):
    pass


class ArchiveFileMeta(FileMeta, ABCMeta):
    def __call__(cls, *args, directory, file, **kwargs):
        cls.lock("|".join([str(directory), str(file)]))
        instance = super(FileMeta, cls).__call__(*args, **kwargs)
        instance.open(*args, **kwargs)
        return instance


class ArchiveFile(ArchiveFileBase, metaclass=ArchiveFileMeta):
    def __init__(self, *args, directory, file, **kwargs):
        self.__directory = directory
        self.__file = file
        self.__source = None
        super().__init__(*args, **kwargs)

    def __repr__(self): return "{}(directory={}, file={})".format(self.__class__.__name__, self.directory, self.file)
    def __str__(self): return "|".join([str(self.directory), str(self.file)])

    @property
    def file(self): return self.__file
    @property
    def directory(self): return self.__directory
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source

    def open(self, *args, mode, archive, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        self.source = archive.open(self.file, mode=mode)
        self.mode = mode

    def execute(self, *args, **kwargs):
        return

    def close(self, *args, **kwargs):
        self.source.close()
        self.source = None
        self.handler = None
        self.mode = None
        self.unlock("|".join([str(self.directory), str(self.file)]))








