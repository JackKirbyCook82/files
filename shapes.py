# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 2022
@name:   ShapeFile Reader/Writer Objects
@author: Jack Kirby Cook

"""

# import fiona
import os.path
from abc import ABC
from zipfile import Path
from collections import OrderedDict as ODict

from utilities.meta import RegistryMeta
from utilities.shapes import Shape

from files.files import File

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["ShapeRecord", "ShapeFile"]
__copyright__ = "Copyright 2022, Jack Kirby Cook"
__license__ = ""


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


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

    def serialize(self, *args, **kwargs):
        geometry = self.shape.serialize()
        properties = ODict([(key, value) for key, value in self.record.items()])
        return {"geometry": geometry, "properties": properties}


class ShapeFile(File):
    def __init__(self, *args, file, **kwargs):
        assert str(file).endswith(".shp")
        archive, file = self.split(file)
        path = Path(archive, file)
        uri = "zip://{}!{}".format(path.root.filename, path.name)
        super().__init__(*args, file=file, **kwargs)
        self.__uri = uri

    @staticmethod
    def split(file):
        head, tail = os.path.split(file)
        name, ext = os.path.splitext(tail)
        archive = os.path.join(head, ".".join([name, "zip"]))
        file = ".".join([name, ext])
        return archive, file

    def getSource(self, *args, mode, driver=None, crs=None, geometry=None, fields={}, **kwargs):
        schema = dict(geometry=geometry, properties=fields) if geometry is not None else None
        return fiona.open(self.uri, mode=mode, driver=driver, crs=crs, schema=schema)

    def getHandler(self, *args, mode, **kwargs):
        return ShapeHandler[mode](self.source, *args, **kwargs)

    @property
    def uri(self): return self.__uri


class ShapeHandler(ABC, metaclass=RegistryMeta):
    def __init__(self, source, *args, geometry, fields, **kwargs):
        assert isinstance(fields, list)
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
        contents = shaperecord.serialize()
        assert contents["geometry"]["type"] == self.geometry
        assert tuple(contents["properties"].keys() == self.fields)
        self.source.write(contents)


class ShapeAppender(ShapeWriter, key="a"):
    def __init__(self, source, *args, **kwargs):
        geometry = source.schema["geometry"]
        fields = tuple(source.schema["properties"].keys())
        assert kwargs["geometry"] == geometry if "geometry" in kwargs.keys() else True
        assert tuple(kwargs["fields"]) == fields if "fields" in kwargs.keys() else True
        super().__init__(source, *args, geometry=geometry, fields=fields, **kwargs)

