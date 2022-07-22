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


class DataframeFormatError(Exception): pass


class DataframeRecordMeta(RegistryMeta, ABCMeta):
    def __init__(cls, *args, **kwargs):
        cls.__fileformat__ = kwargs.get("fileformat", getattr(cls, "__fileformat__", "csv"))
        cls.__archiveformat__ = kwargs.get("archiveformat", getattr(cls, "__archiveformat__", "zip"))
        super(DataframeRecordMeta, cls).__init__(*args, **kwargs)

    def __call__(cls, *args, file, mode, **kwargs):
        archive, file = cls.archivefile(file)
        dataframe = cls.load(*args, file=file, archive=archive, **kwargs) if mode in ("r", "a") else pd.DataFrame()
        instance = super(DataframeRecordMeta, cls[mode]).__call__(dataframe, *args, file=file, archive=bool(archive), **kwargs)
        return instance

    @staticmethod
    def load(*args, file, archive=None, index=False, header=True, **kwargs):
        index = None if not index else 0
        header = None if not header else 0
        compression = dict(method="zip", archive_name=file) if archive is not None else None
        file = archive if archive is not None else file
        try:
            dataframe = pd.read_csv(file, compression=compression, index_col=index, header=header, na_values=NAN_FROMFILE).dropna(axis=0, how="all")
        except pd.errors.EmptyDataError:
            dataframe = pd.DataFrame()
        dataframe = dataframe.to_frame() if not isinstance(dataframe, pd.DataFrame) else dataframe
        return dataframe

    @property
    def fileformat(cls): return cls.__fileformat__
    @property
    def archiveformat(cls): return cls.__archiveformat__

    def archivefile(cls, file):
        head, tail = os.path.split(file)
        name, extension = str(tail).split(".")
        if extension == cls.archiveformat:
            archive = os.path.join(head, tail)
            file = ".".join([name, cls.fileformat])
        elif extension == cls.fileformat:
            archive = None
            file = os.path.join(head, tail)
        else:
            raise DataframeFormatError(str(file))
        return archive, file


class DataframeRecord(ABC, metaclass=DataframeRecordMeta, fileformat="csv", archiveformat="zip"):
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
    @dataframe.setter
    def dataframe(self, dataframe): self.__dataframe = dataframe
    @property
    def file(self): return self.__file
    @property
    def archive(self): return self.__archive
    @property
    def archived(self): return self.__archive is not None
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
        compression = dict(method="zip", archive_name=self.file) if self.archived else None
        file = self.archive if self.archived else self.file
        dataframe = self.dataframe.replace(inplace=False, to_replace=NAN_TOFILE, value=np.nan)
        dataframe.to_csv(file, compression=compression, index=self.index, header=self.header)

    def close(self, *args, **kwargs):
        if bool(self):
            self.save()
        self.dataframe = pd.DataFrame()


class DataframeFile(File):
    def getSource(self, *args, mode, **kwargs): return DataframeRecord(*args, file=self.file, mode=mode, **kwargs)
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


class DataframeWriter(DataframeHandler, key=("w", "x", "a")):
    def __call__(self, dataframe, index=None, header=None):
        if index is not None:
            dataframe = dataframe.set_index(index, drop=True, inplace=False)
        if header is not None:
            dataframe = dataframe[_aslist(header)]
        self.source.write(dataframe)

