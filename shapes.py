# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 2022
@name:   ShapeFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import fiona
from abc import ABC
from fiona.io import ZipMemoryFile
from collections import OrderedDict as ODict

from utilities.meta import RegistryMeta
from utilities.shapes import Shape

from files.files import File
from files.archives import Archive

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ShapeRecord", "ShapeCollection", "ShapeFile"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]
_function = lambda file, *a, mode, driver, crs, schema, **kw: fiona.open(file, mode=mode, driver=driver, crs=crs, schema=schema)


class ShapeRecord(object):
    def __init__(self, shape, record):
        self.__shape = shape
        self.__record = record

    @property
    def shape(self): return self.__shape
    @property
    def record(self): return self.__record

    @classmethod
    def deserialize(cls, contents, *args, fields=None, **kwargs):
        fields = fields if fields is not None else contents["properties"].keys()
        shape = Shape.deserialize(contents["geometry"])
        record = ODict([(key, value) for key, value in contents["properties"] if key in fields])
        return cls(shape, record)

    def serialize(self, *args, fields, **kwargs):
        geometry = self.shape.serialize()
        properties = ODict([(field, self.record.get(field, None)) for field in fields])
        return {"geometry": geometry, "properties": properties}


class ShapeCollection(list):
    def __init__(self, shaperecords=[]):
        assert all([isinstance(shaperecord, ShapeRecord) for shaperecord in _aslist(shaperecords)])
        super().__init__(_aslist(shaperecords))

    def __add__(self, other):
        assert isinstance(other, type(self))
        return self.__class__([*self, *other])


class ShapeFile(File, function=_function):
    def __init__(self, *args, driver=None, crs=None, geometry=None, header=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__driver = driver
        self.__crs = crs
        self.__geometry = geometry
        self.__header = header

    @property
    def driver(self): return self.__driver
    @property
    def crs(self): return self.__crs
    @property
    def geometry(self): return self.__geometry
    @property
    def header(self): return self.__header
    @property
    def schema(self): return {"geometry": self.geometry, "properties": self.header}
    @property
    def parameters(self): return {"driver": self.driver, "crs": self.crs, "schema": self.schema}

    def open(self, *args, **kwargs):
        super().open(*args, **self.parameters, **kwargs)

    def execute(self, *args, **kwargs):
        return ShapeHandler[self.mode](self.source, *args, **kwargs)


# class ShapeArchive(Archive, Shape):
#     def execute(self, *args, **kwargs):
#         assert "file" in kwargs.keys() and "mode" in kwargs.keys()
#         source = self.source(*args, **kwargs)
#         function = lambda file, *a, mode, driver, crs, schema, **kw: ZipMemoryFile(source).open(file, mode=mode, driver=driver, crs=crs, schema=schema)
#         return ShapeFile(*args, function=function, geometry=self.geometry, fields=self.fields, **kwargs)


class ShapeHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, geometry, header, fields=None, **kwargs):
        assert isinstance(fields, (list, type(None)))
        assert isinstance(header, list)
        fields = tuple([field if field in header else None for field in fields]) if fields is not None else tuple(header)
        self.__source = source
        self.__geometry = geometry
        self.__header = header
        self.__fields = fields

    @property
    def source(self): return self.__source
    @property
    def geometry(self): return self.__geometry
    @property
    def header(self): return self.__header
    @property
    def fields(self): return self.__fields


class ShapeReader(ShapeHandler, key="r"):
    def __init__(self, source, *args, **kwargs):
        geometry = source.schema["geometry"]
        header = tuple(source.schema["properties"].keys())
        assert kwargs["geometry"] == geometry if "geometry" in kwargs.keys() else True
        assert tuple(kwargs["header"]) == header if "header" in kwargs.keys() else True
        super().__init__(source, *args, geometry=geometry, header=header, **kwargs)

    def __next__(self):
        contents = next(self.source)
        assert contents["geometry"]["type"] == self.geometry
        assert tuple(contents["properties"].keys()) == self.header
        return ShapeRecord.deserialize(contents, fields=self.fields)

    def __iter__(self):
        return self


class ShapeWriter(ShapeHandler, key=("w", "r")):
    def __call__(self, shaperecord):
        assert isinstance(shaperecord, ShapeRecord)
        contents = shaperecord.serialize(fields=self.fields)
        assert contents["geometry"]["type"] == self.geometry
        assert tuple(contents["properties"].keys() == self.header)
        self.source.write(contents)


class ShapeAppender(ShapeWriter, key="a"):
    def __init__(self, source, *args, **kwargs):
        geometry = source.schema["geometry"]
        header = tuple(source.schema["properties"].keys())
        assert kwargs["geometry"] == geometry if "geometry" in kwargs.keys() else True
        assert tuple(kwargs["header"]) == header if "header" in kwargs.keys() else True
        super().__init__(source, *args, geometry=geometry, header=header, **kwargs)

