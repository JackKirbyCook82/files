# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 2018
@name:   DataFrameFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
import pandas as pd
import numpy as np
from abc import ABC

from utilities.meta import RegistryMeta

from files.files import File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["DataframeCollection", "DataframeFile"]
__copyright__ = "Copyright 2018, Jack Kirby Cook"
__license__ = ""


NAN_TOFILE = ["None", None, np.NaN, ""]
NAN_FROMFILE = ["", "nan", "NaN", "NA", "N/A", "None"]


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]
_concat = lambda dataframes: pd.concat(dataframes, axis=0, ignore_index=True).drop_duplicates(inplace=True, ignore_index=True, keep="last")
_function = lambda file, *a, mode, directory=None, **kw: DataframeRecord.load(file, *a, archive=directory, **kw) if mode in ("r", "a") else DataframeRecord.empty(*a, *kw)


class DataframeRecord(object):
    def __init__(self, dataframe, parsers={}, parser=None):
        assert isinstance(dataframe, (pd.DataFrame, pd.Series))
        dataframe = dataframe.to_frame() if isinstance(dataframe, pd.Series) else dataframe
        self.__dataframe = self.parse(dataframe, parsers=parsers, parser=parser)

    def __bool__(self): return not self.dataframe.empty
    def __len__(self): return len(self.dataframe.index)

    def __getitem__(self, columns):
        self.dataframe = self.dataframe[list(columns)]
        return self

    def __add__(self, other):
        assert isinstance(other, type(self))
        dataframes = _filter([self.dataframe, other.dataframe])
        dataframe = _concat(dataframes)
        return self.__class__(dataframe, parsers={}, parser=None)

    def __iadd__(self, other):
        assert isinstance(other, type(self))
        dataframes = _filter([self.dataframe, other.dataframe])
        dataframe = _concat(dataframes)
        self.dataframe = dataframe
        return self

    @property
    def dataframe(self): return self.__dataframe
    @dataframe.setter
    def dataframe(self, dataframe): self.__dataframe = dataframe

    @staticmethod
    def parse(dataframe, parsers={}, parser=None):
        for column in dataframe.columns:
            parser = parsers.get(column, parser)
            if parser is not None:
                dataframe[column] = dataframe[column].apply(parser)
        return dataframe

    def save(self, file, *args, archive=None, index=False, header=True, **kwargs):
        if not bool(self):
            return
        compression = dict(method="zip", archive_name=file) if archive is not None else None
        file = archive if archive is not None else file
        dataframe = self.dataframe.replace(to_replace=NAN_TOFILE, value=np.nan)
        dataframe.to_csv(file, compression=compression, index=index, header=header)

    @classmethod
    def load(cls, file, *args, archive=None, index=False, header=True, **kwargs):
        index = None if not index else 0
        header = None if not header else 0
        compression = dict(method="zip", archive_name=file) if archive is not None else None
        file = archive if archive is not None else file
        dataframe = pd.read_csv(file, compression=compression, index_col=index, header=header, na_values=NAN_FROMFILE).dropna(axis=0, how="all")
        dataframe = dataframe.to_frame() if not isinstance(dataframe, pd.DataFrame) else dataframe
        return cls(dataframe, *args, **kwargs)

    @classmethod
    def empty(cls, *args, **kwargs):
        return cls(pd.DataFrame(), args, **kwargs)


class DataframeCollection(list):
    def __init__(self, dataframerecords=[]):
        assert all([isinstance(dataframerecord, DataframeRecord) for dataframerecord in _aslist(dataframerecords)])
        super().__init__(_aslist(dataframerecords))

    def __add__(self, other):
        assert isinstance(other, type(self))
        return self.__class__([*self, *other])


class DataframeFile(File):
    def __init__(self, *args, file, index=False, header=True, parsers={}, parser=None, **kwargs):
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
        super().__init__(*args, file=file, **kwargs)
        self.__directory = directory
        self.__index = index
        self.__header = header
        self.__parsers = parsers
        self.__parser = parser

    def __repr__(self):
        string = "{cls}(directory={directory})" if self.directory is not None else "{cls}(file={file})"
        return str(string).format(self.__class__.__name__, self.directory, self.file)

    def __str__(self):
        string = str(self.directory) if self.directory is not None else str(self.file)
        return string

    @property
    def directory(self): return self.__directory
    @property
    def index(self): return self.__index
    @property
    def header(self): return self.__header
    @property
    def parsers(self): return self.__parsers
    @property
    def parser(self): return self.__parser
    @property
    def parameters(self): return {"index": self.index, "header": self.header, "parsers": self.parsers, "parser": self.parser}

    def execute(self, *args, **kwargs):
        return DataframeHandler[self.mode](self.source, *args, **kwargs)

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        if mode in ("r", "a") and not os.path.exist(self.file):
            raise FileNotFoundError(str(self.file))
        elif mode == "x" and os.path.exist(self.file):
            raise FileExistsError(str(self.file))
        super().open(*args, mode=mode, **self.parameters, **kwargs)

    def close(self, *args, **kwargs):
        if self.mode in ("w", "a", "x"):
            self.source.save(self.file, archive=self.directory)
        super().close(*args, **kwargs)


class DataframeHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, fields=None, **kwargs):
        assert isinstance(fields, (list, type(None)))
        header = tuple(source.dataframe.columns)
        fields = tuple([field if field in header else None for field in fields]) if fields is not None else tuple(header)
        self.__source = source[fields]
        self.__header = header
        self.__fields = fields

    @property
    def source(self): return self.__source
    @property
    def header(self): return self.__header
    @property
    def fields(self): return self.__fields


class DataframeReader(DataframeHandler, key="r"):
    def __call__(self): return self.source[self.fields]


class DataframeWriter(DataframeHandler, key=("w", "r", "a")):
    def __call__(self, other): self.source += other[self.fields]


