# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   Archive Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
from io import BytesIO
from zipfile import ZipFile

from utilities.meta import RegistryMeta

from files.files import File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Archive"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]
_source = lambda file, *a, mode, **kw: ZipFile.open(file, mode=mode)


class Archive(File, source=_source):
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        cls.copy(instance.source, instance.destination, exclude=[])
        return instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__destination = None

    @property
    def destination(self): return self.__destination
    @destination.setter
    def destination(self, destination): self.__destination = destination

    @staticmethod
    def copy(source, destination, exclude=[]):
        for file in source.namelist():
            if file not in _filter(exclude):
                content = source.read(file)
                destination.writestr(file, content)

#    def execute(self, *args, **kwargs):
#        archive = self.source if self.readable else self.destination
#        return ArchiveHandler[self.mode](archive, *args, **kwargs)

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(self.file):
            raise FileNotFoundError(str(self.file))
        elif mode == "x" and os.path.exist(self.file):
            raise FileExistsError(str(self.file))
        super().open(*args, mode=mode, **kwargs)
        self.destination = ZipFile(BytesIO(), mode="w")

    def close(self, *args, **kwargs):
        super().close(*args, **kwargs)
        if self.writeable:
            with ZipFile(self.file, mode="w") as archive:
                content = self.destination.getbuffer()
                archive.write(content)
        self.destination.close()
        self.destination = None


# class ArchiveHandler(ABC, metaclass=RegistryMeta):
#     def __init__(self, archive, *args, **kwargs):
#         self.__archive = archive
#         self.__source = None
#
#     def __iter__(self): return (file for file in self.archive.namelist())
#     def __contains__(self, file): return file in self.archive.namelist()
#     def __call__(self, file, *args, **kwargs): return self.execute(file, *args, **kwargs)
#
#     @property
#     def archive(self): return self.__archive
#     @property
#     def source(self): return self.__source
#     @source.setter
#     def source(self, source): self.__source = source
#     @abstractmethod
#     def execute(self, file, *args, **kwargs): pass
#
#
# class ArchiveReader(ArchiveHandler, key="r"):
#     def __getitem__(self, file): return self.archive.read(file)
#
#     def execute(self, file, *args, **kwargs):
#         self.source = self.archive.open(file, mode=self.mode)
#
#
# class ArchiveWriter(ArchiveHandler, keys=("w", "x", "a")):
#     def __setitem__(self, file, content): self.archive.writestr(file, content)
#
#     def execute(self, file, *args, **kwargs):
#         self.source = self.archive.open(file, mode=self.mode)




