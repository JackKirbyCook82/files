# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 2018
@name:   DataFrameFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
import pandas as pd
import numpy as np
from abc import ABC, ABCMeta

from utilities.meta import RegistryMeta

from files.files import FileMeta, FileBase

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["DataframeFile"]
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


NAN_TOFILE = ["None", None, np.NaN, ""]
NAN_FROMFILE = ["", "nan", "NaN", "NA", "N/A", "None"]


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]
_concat = lambda dataframes: pd.concat(dataframes, axis=0, ignore_index=True).drop_duplicates(inplace=True, ignore_index=True, keep="last")


class DataframeRecord(object):
    def __init__(self, dataframe=None):
        self.__dataframe = dataframe

    def __call__(self, dataframe=None):
        dataframes = _filter([self.dataframe, dataframe])
        dataframe = _concat(dataframes)
        self.__dataframe = dataframe

    @property
    def dataframe(self): return self.__dataframe

    @classmethod
    def load(cls, file, *args, archive=None, **kwargs):
        compression = dict(method="zip", archive_name=file) if archive is not None else None
        file = archive if archive is not None else file
        dataframe = pd.read_csv(file, compression=compression, index_col=None, header=0, na_values=NAN_FROMFILE).dropna(axis=0, how="all")
        dataframe = dataframe.to_frame() if not isinstance(dataframe, pd.DataFrame) else dataframe
        return cls(dataframe)

    def save(self, file, *args, archive=None, **kwargs):
        compression = dict(method="zip", archive_name=file) if archive is not None else None
        file = archive if archive is not None else file
        dataframe = self.dataframe.replace(to_replace=NAN_TOFILE, value=np.nan)
        dataframe.to_csv(file, compression=compression, index=False, header=True)


class DataframeBase(FileBase, ABC): pass
class DataframeMeta(FileMeta, ABCMeta): pass


class DataframeFile(DataframeBase, metaclass=DataframeMeta, key="file"):
    def __init__(self, *args, file, **kwargs):
        head, tail = os.path.split(file)
        name, ext = str(tail).split(".")
        if ext == "zip":
            directory = os.path.join(head, ".".join([name, "zip"]))
            file = ".".join([tail, "csv"])
        elif ext == "csv":
            directory = None
            file = os.path.join(head, ".".join([name, "csv"]))
        else:
            raise ValueError(file)
        self.__directory = directory
        self.__file = file
        self.__source = None
        super().__init__(*args, **kwargs)

    def __repr__(self):
        string = "{cls}(directory={directory})" if self.directory is not None else "{cls}(file={file})"
        return str(string).format(self.__class__.__name__, self.directory, self.file)

    def __str__(self):
        string = str(self.directory) if self.directory is not None else str(self.file)
        return string

    @property
    def directory(self): return self.__directory
    @property
    def file(self): return self.__file
    @property
    def source(self): return self.__source
    @source.setter
    def source(self, source): self.__source = source

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(self.file):
            raise FileNotFoundError(str(self.file))
        elif mode == "x" and os.path.exist(self.file):
            raise FileExistsError(str(self.file))
        self.source = DataframeRecord.load(self.file, archive=self.directory) if mode in ("r", "a") else DataframeRecord()
        self.mode = mode

    def execute(self, *args, **kwargs):
        return DataframeHandler[self.mode](self.source, *args, **kwargs)

    def close(self, *args, **kwargs):
        if self.mode in ("w", "a", "x"):
            self.source.save(self.file, archive=self.directory)
        self.source.close()
        self.source = None
        self.handler = None
        self.mode = None
        self.unlock()


class DataframeHandler(object, metaclass=RegistryMeta):
    def __init__(self, source, *args, **kwargs): self.__source = source
    @property
    def source(self): return self.source


class DataframeReader(DataframeHandler, key="r"):
    def __call__(self): return self.source.dataframe


class DataframeWriter(DataframeHandler, key=("w", "r", "a")):
    def __call__(self, dataframe): self.source(dataframe)

