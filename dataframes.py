# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 2018
@name:   DataFrameFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import os.path
import pandas as pd
import numpy as np

from files.files import File
from utilities.meta import RegistryMeta

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
_combine = lambda dataframes: pd.concat(_filter(list(dataframes), None), axis=0, ignore_index=True).drop_duplicates(inplace=True, ignore_index=True, keep="last")


# class DataframeRecord(object):
#     def __init__(self, dataframe): self.__dataframe = dataframe
#     def __call__(self, dataframe): self.__dataframe = _combine(dataframe)
#
#     @property
#     def dataframe(self): return self.__dataframe
#     @classmethod
#     def empty(cls, *args, **kwargs): return cls(None)
#
#     @classmethod
#     def load(cls, file, *args, archive, **kwargs):
#         compression = dict(method="zip", archive_name=file) if archive is not None else None
#         file = archive if archive is not None else file
#         dataframe = pd.read_csv(file, compression=compression, index_col=None, header=0, na_values=NAN_FROMFILE).dropna(axis=0, how="all")
#         dataframe = dataframe.to_frame() if not isinstance(dataframe, pd.DataFrame) else dataframe
#         return cls(dataframe)
#
#     def save(self, file, *args, archive=None, **kwargs):
#         compression = dict(method="zip", archive_name=file) if archive is not None else None
#         file = archive if archive is not None else file
#         dataframe = self.dataframe.replace(to_replace=NAN_TOFILE, value=np.nan)
#         dataframe.to_csv(file, compression=compression, index=False, header=True)


class DataframeFile(File):
    def __init__(self, *args, file, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise KeyError(mode)
        if not str(file).endswith(".zip") and not str(file).endswith(".csv"):
            raise ValueError(str(file))
        if mode in ("r", "a") and not os.path.exist(file):
            raise FileNotFoundError(str(file))
        elif mode == "x" and os.path.exist(file):
            raise FileExistsError(str(file))
        else:
            pass
        super().__init__(*args, **kwargs)

    @property
    def archivefile(self):
        directory, base = os.path.split(self.file)
        filename, fileext = str(base).split(".")
        if str(base).endswith(".zip"):
            archive = os.path.join(directory, ".".join([filename, "zip"]))
            file = ".".join([base, "csv"])
        else:
            archive = None
            file = os.path.join(directory, ".".join([base, "csv"]))
        return archive, file

    def open(self, *args, mode, **kwargs):
        if mode not in ("r", "w", "a", "x"):
            raise ValueError(mode)
        archive, file = self.archivefile
        self.source = DataframeRecord.load(file, archive=archive) if self.mode in ("r", "a") else DataframeRecord.empty()
        self.mode = mode

    def execute(self, *args, **kwargs):
        return DataframeHandler[self.mode](self.source, *args, **kwargs)

    def close(self, *args, **kwargs):
        archive, file = self.archivefile
        if self.mode in ("w", "a", "x"):
            self.source.save(file, archive=archive)
        self.source.close()
        self.source = None
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

