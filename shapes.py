# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 2022
@name:   ShapeFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

import fiona
from abc import ABC
from collections import OrderedDict as ODict

from utilities.meta import RegistryMeta
from utilities.shapes import Shape

from files.files import File
from files.archives import Archive

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ShapeFile"]
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
    def deserialize(cls, contents, *args, **kwargs):
        shape = Shape.deserialize(contents["geometry"])
        record = ODict([(key, value) for key, value in contents["properties"]])
        return cls(shape, record)

    def serialize(self, *args, fields, **kwargs):
        geometry = self.shape.serialize()
        properties = ODict([(field, self.record.get(field, None)) for field in fields])
        return {"geometry": geometry, "properties": properties}


class ShapeFile(File, function=_function):
    def __init__(self, *args, driver=None, crs=None, geometry=None, fields=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__driver = driver
        self.__crs = crs
        self.__geometry = geometry
        self.__fields = fields

    @property
    def driver(self): return self.__driver
    @property
    def crs(self): return self.__crs
    @property
    def geometry(self): return self.__geometry
    @property
    def fields(self): return self.__fields
    @property
    def schema(self): return {"geometry": self.geometry, "properties": self.fields}
    @property
    def parameters(self): return {"driver": self.driver, "crs": self.crs, "schema": self.schema}

    def execute(self, *args, **kwargs):
        return ShapeHandler[self.mode](self.source, *args, geometry=self.geometry, fields=self.fields, **kwargs)


class ShapeArchive(Archive):
    pass

#    def execute(self, *args, **kwargs):
#        pass


class ShapeHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, geometry, fields, **kwargs):
        self.__source = source
        self.__geometry = geometry
        self.__fields = fields

    @property
    def source(self): return self.__source
    @property
    def geometry(self): return self.__geometry
    @property
    def fields(self): return self.__fields


class ShapeReader(ShapeHandler, key="r"):
    def __init__(self, source, *args, **kwargs):
        geometry = source.schema["geometry"]
        fields = tuple(source.schema["properties"].keys())
        assert kwargs["geometry"] == geometry if "geometry" in kwargs.keys() else True
        assert tuple(kwargs["fields"]) == fields if "fields" in kwargs.keys() else True
        super().__init__(source, *args, geometry=geometry, fields=fields, **kwargs)

    def __next__(self):
        contents = next(self.source)
        assert contents["geometry"]["type"] == self.geometry
        assert tuple(contents["properties"].keys()) == self.fields
        return ShapeRecord.deserialize(contents)

    def __iter__(self):
        return self


class ShapeWriter(ShapeHandler, key=("w", "r")):
    def __call__(self, shaperecord):
        assert isinstance(shaperecord, ShapeRecord)
        contents = shaperecord.serialize(fields=self.fields)
        assert contents["geometry"]["type"] == self.geometry
        assert tuple(contents["properties"].keys() == self.fields)
        self.source.write(contents)


class ShapeWriter(ShapeWriter, key="a"):
    def __init__(self, source, *args, **kwargs):
        geometry = source.schema["geometry"]
        fields = tuple(source.schema["properties"].keys())
        assert kwargs["geometry"] == geometry if "geometry" in kwargs.keys() else True
        assert tuple(kwargs["fields"]) == fields if "fields" in kwargs.keys() else True
        super().__init__(source, *args, geometry=geometry, fields=fields, **kwargs)

