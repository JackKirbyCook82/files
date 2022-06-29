# -*- coding: utf-8 -*-
"""
Created on Weds Jan 12 2022
@name:   CSVFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import csv
from abc import ABC

from utilities.meta import RegistryMeta

from files.files import File, FileLocation
from files.archives import Archive

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["CSVFile"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_flatten = lambda y: [i for x in y for i in x]


class CSVFile(File):
    def __init__(self, *args, fields=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__fields = fields

    @property
    def fields(self): return self.__fields

    def execute(self, *args, **kwargs):
        return CSVHandler[self.mode](self.source, *args, **kwargs)


class CSVArchive(Archive):
    def execute(self, *args, **kwargs):
        assert "file" in kwargs.keys() and "mode" in kwargs.keys()
        source = self.source(*args, **kwargs)
        function = lambda file, *a, mode, **kw: source.open(file, mode=mode)
        return CSVFile(*args, function=function, **kwargs)


class CSVHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, fields=None, header, **kwargs):
        assert isinstance(fields, (list, type(None)))
        assert isinstance(header, list)
        fields = tuple([field if field in header else None for field in fields]) if fields is not None else tuple(header)
        self.__source = source
        self.__header = header
        self.__fields = fields

    @property
    def source(self): return self.__source
    @property
    def header(self): return self.__header
    @property
    def fields(self): return self.__fields


class CSVReader(CSVHandler, key="r"):
    def __init__(self, source, *args, **kwargs):
        reader = csv.reader(source)
        reader.seek(0, FileLocation.START)
        header = next(reader)
        super().__init__(reader, *args, header=header, **kwargs)

    def __next__(self): return {key: value for key, value in zip(self.fields, next(self.source)) if key is not None}
    def __iter__(self): return self


class CSVWriter(CSVHandler, keys=("w", "x")):
    def __init__(self, source, *args, fields, **kwargs):
        writer = csv.writer(source)
        writer.seek(0, FileLocation.START)
        writer.writerow(fields)
        super().__init__(writer, *args, header=fields, fields=fields, **kwargs)

    def __call__(self, contents):
        assert isinstance(contents, dict)
        row = [contents.get(field, None) if field is not None else None for field in self.fields]
        self.source.writerow(row)


class CSVAppender(CSVHandler, key="a"):
    def __init__(self, source, *args, **kwargs):
        reader = csv.reader(source)
        reader.seek(0, FileLocation.START)
        header = next(reader)
        writer = csv.writer(source)
        writer.seek(0, FileLocation.STOP)
        super().__init__(writer, *args, header=header, **kwargs)

    def __call__(self, contents):
        assert isinstance(contents, dict)
        row = [contents.get(field, None) if field is not None else None for field in self.fields]
        self.source.writerow(row)



