# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 2018
@name:   DataFrameFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
import pandas as pd
import numpy as np
from abc import ABC, ABCMeta, abstractmethod

from utilities.meta import RegistryMeta

from files.files import File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["DataframeRecord", "DataframeFile"]
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


NAN_TOFILE = ["None", None, np.NaN, ""]
NAN_FROMFILE = ["", "nan", "NaN", "NA", "N/A", "None"]


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]
_concat = lambda dataframes: pd.concat(dataframes, axis=0, ignore_index=True)
_drop = lambda dataframe: dataframe.drop_duplicates(inplace=False, ignore_index=True, keep="last")


class DataframeRecordMeta(RegistryMeta, ABCMeta):
    def __call__(cls, file, *args, archive=True, mode, **kwargs):
        assert isinstance(archive, bool)
        archive, file = cls.split(file) if bool(archive) else (None, file)
        dataframe = cls.load(*args, file=file, archive=archive, **kwargs) if mode in ("r", "a") else pd.DataFrame()
        instance = super(DataframeRecordMeta, cls[mode]).__call__(dataframe, *args, **kwargs)
        return instance

    @staticmethod
    def load(*args, file, archive=None, index=False, header=True, **kwargs):
        index = None if not index else 0
        header = None if not header else 0
        compression = dict(method="zip", archive_name=file) if archive is not None else None
        file = archive if archive is not None else file
        dataframe = pd.read_csv(file, compression=compression, index_col=index, header=header, na_values=NAN_FROMFILE).dropna(axis=0, how="all")
        dataframe = dataframe.to_frame() if not isinstance(dataframe, pd.DataFrame) else dataframe
        return dataframe

    @staticmethod
    def split(file):
        head, tail = os.path.split(file)
        name, ext = os.path.splitext(tail)
        archive = os.path.join(head, ".".join([name, "zip"]))
        file = ".".join([name, ext])
        return archive, file


class DataframeRecord(ABC, metaclass=DataframeRecordMeta):
    def __init__(self, dataframe, *args, file, archive=None, index=False, header=True, parsers={}, parser=None, **kwargs):
        assert isinstance(dataframe, (pd.DataFrame, pd.Series))
        self.__file = file
        self.__archive = archive
        self.__index = index
        self.__header = header
        self.__parsers = parsers
        self.__parser = parser
        dataframe = dataframe.to_frame() if isinstance(dataframe, pd.Series) else dataframe
        dataframe = self.parse(dataframe)
        self.__dataframe = dataframe

    def __bool__(self): return not self.dataframe.empty
    def __len__(self): return len(self.dataframe.index)

    @property
    def dataframe(self): return self.__dataframe
    @property
    def file(self): return self.__file
    @property
    def archive(self): return self.__archive
    @property
    def index(self): return self.__index
    @property
    def header(self): return self.__header
    @property
    def parsers(self): return self.__parsers
    @property
    def parser(self): return self.__parser

    def parse(self, dataframe):
        for column in dataframe.columns:
            parser = self.parsers.get(column, self.parser)
            if parser is not None:
                dataframe[column] = dataframe[column].apply(parser)
        return dataframe

    @abstractmethod
    def close(self, *args, **kwargs): pass


class ReaderDataframeRecord(DataframeRecord, key="r"):
    def read(self): return self.dataframe
    def close(self, *args, **kwargs): self.dataframe = pd.DataFrame()


class WriterDataframeRecord(DataframeRecord, keys=("w", "x", "a")):
    def write(self, dataframe):
        dataframe = dataframe.to_frame() if isinstance(dataframe, pd.Series) else dataframe
        dataframe = self.parse(dataframe)
        dataframe = _concat([self.dataframe, dataframe])
        dataframe = _drop(dataframe)
        self.dataframe = dataframe

    def save(self):
        compression = dict(method="zip", archive_name=self.file) if self.archive is not None else None
        file = self.archive if self.archive is not None else self.file
        dataframe = self.dataframe.replace(inplace=False, to_replace=NAN_TOFILE, value=np.nan)
        dataframe.to_csv(file, compression=compression, index=self.index, header=self.header)

    def close(self, *args, **kwargs):
        if bool(self):
            self.save()
        self.dataframe = pd.DataFrame()


class DataFrameBase(object):
    def __init__(self, *args, index=False, header=True, parsers={}, parser=None, **kwargs):
        self.__index = index
        self.__header = header
        self.__parsers = parsers
        self.__parser = parser

    @property
    def parameters(self): return {"index": self.index, "header": self.header, "parsers": self.parsers, "parser": self.parser}
    @property
    def index(self): return self.__index
    @property
    def header(self): return self.__header
    @property
    def parsers(self): return self.__parsers
    @property
    def parser(self): return self.__parser


class DataframeFile(File, DataFrameBase):
    def getSource(self, *args, mode, **kwargs): return DataframeRecord(self.file, *args, archive=True, mode=mode, **self.parameters, **kwargs)
    def getHandler(self, *args, mode, **kwargs): return DataframeHandler[mode](self.source, *args, **kwargs)


class DataframeHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, **kwargs): self.__source = source
    @property
    def source(self): return self.__source


class DataframeReader(DataframeHandler, key="r"):
    def __call__(self, index=None, header=None):
        dataframe = self.source.read()
        if index is not None:
            dataframe = dataframe.set_index(index, drop=True, inplace=False)
        if header is not None:
            dataframe = dataframe[_aslist(header)]
        return dataframe


class DataframeWriter(DataframeHandler, key=("w", "r", "a")):
    def __call__(self, dataframe, index=None, header=None):
        if index is not None:
            dataframe = dataframe.set_index(index, drop=True, inplace=False)
        if header is not None:
            dataframe = dataframe[_aslist(header)]
        self.source.write(dataframe)

