# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   Archive Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from io import BytesIO
from zipfile import ZipFile

from utilities.dispatchers import keyword_single_dispatcher as dispatcher

from files.files import FileMeta, File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Archive"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


class Archive(File, metaclass=FileMeta):
    def __init__(self, *args, **kwargs):
        self.__destination = None
        super().__init__(*args, **kwargs)

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

    def close(self, *args, **kwargs):
        self.source.close()
        self.source = None
        if self.writeable:
            with ZipFile(self.file, mode="w") as archive:
                content = self.destination.getbuffer()
                archive.write(content)
        self.destination.close()
        self.destination = None
        self.mode = None
        self.unlock(str(self))

    @staticmethod
    def copy(source, destination, exclude=[]):
        assert isinstance(exclude, list)
        for file in source.namelist():
            if file not in exclude:
                content = source.read(file)
                destination.writestr(file, content)


class ArchiveFile(File):
    def __init__(self, *args, directory, **kwargs):
        super().__init__(*args, **kwargs)
        self.__directory = directory

    def __repr__(self): return "{}(directory={}, file={})".format(self.__class__.__name__, self.directory, self.file)
    def __str__(self): return "|".join([str(self.directory), str(self.file)])

    @property
    def directory(self): return self.__directory

    def opener(self, *args, mode, archive, **kwargs):
        return archive.open(self.file, mode=mode)

    def execute(self, *args, **kwargs):
        pass

#    @dispatcher("mode")
#    def execute(self, *args, file, mode, **kwargs): pass

#    @execute.register("r")
#    def execute_readable(self, *args, file, mode, **kwargs):
#        if file not in self.source.namelist():
#            raise FileNotFoundError(str(self.directory), str(self.file))
#        return ArchiveFile(*args, directory=self.directory, file=file, mode=mode, archive=self.source, **kwargs)

#    @execute.register("w", "x", "a")
#    def execute_writeable(self, *args, file, mode, **kwargs):
#        self.destination = ZipFile(BytesIO(), mode="w")
#        if mode == "a" and file not in self.source.namelist():
#            raise FileNotFoundError(str(self.directory), str(self.file))
#        elif mode == "x" and file in self.source.namelist():
#            raise FileExistsError(str(self.directory), str(self.file))
#        if self.mode == "a" and mode == "a":
#            self.copy(self.source, self.destination, exclude=[])
#        elif self.mode == "a" and mode != "a":
#            self.copy(self.source, self.destination, exclude=[file])
#        return ArchiveFile(*args, directory=self.directory, file=file, mode=mode, archive=self.destination, **kwargs)






