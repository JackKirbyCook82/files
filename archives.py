# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   File Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from io import BytesIO
from zipfile import ZipFile

from utilities.dispatchers import keyword_single_dispatcher as dispatcher

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
        self.__destination = None

#    def __call__(self, *args, **kwargs):
#        pass

    @property
    def destination(self): return self.__destination
    @destination.setter
    def destination(self, destination): self.__destination = destination

    def open(self, *args, mode, **kwargs):
        if bool(self):
            raise OpenedArchiveError(str(self))
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(self.file):
            raise FileNotFoundError(str(self.file))
        elif mode == "x" and os.path.exist(self.file):
            raise FileExistsError(str(self.file))
        self.source = self.function(self.file, *args, mode=mode, **kwargs)
        if self.writeable:
            self.destination = ZipFile(BytesIO(), mode="w")
        self.mode = mode

    def close(self, *args, **kwargs):
        if not bool(self):
            raise ClosedArchiveError(str(self))
        self.source.close()
        self.source = None
        if self.writeable:
            with ZipFile(self.directory, mode="w") as archive:
                content = self.destination.getbuffer()
                archive.write(content)
            self.destination.close()
            self.destination = None
        self.mode = None
        self.unlock(str(self))

    @dispatcher("mode")
    def archive(self, *args, file, mode, **kwargs): pass

    @archive.register("r")
    def reader(self, *args, file, mode="r", **kwargs):
        assert mode == "r"
        if file not in self.source.namelist():
            raise FileNotFoundError(str(self.directory), str(self.file))
        return self.source

    @archive.register("w", "x", "a")
    def writer(self, *args, file, mode, **kwargs):
        assert mode in ("w", "x", "a")
        if mode == "a" and file not in self.source.namelist():
            raise FileNotFoundError(str(self.directory), str(file))
        elif mode == "x" and file in self.source.namelist():
            raise FileExistsError(str(self.directory), str(file))
        if self.mode == "a" and mode == "a":
            self.copy(self.source, self.destination, exclude=[])
        elif self.mode == "a" and mode != "a":
            self.copy(self.source, self.destination, exclude=[file])
        return self.destination

    @staticmethod
    def copy(source, destination, exclude=[]):
        assert isinstance(exclude, list)
        for file in source.namelist():
            if file not in exclude:
                content = source.read(file)
                destination.writestr(file, content)

#    def execute(self, archive, *args, **kwargs):
#        pass


