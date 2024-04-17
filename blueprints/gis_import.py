import os
import geopandas as gpd
import numpy as np
import re
import json
from chardet import detect
from flask import Blueprint, jsonify, request
from sqlalchemy import Table, Column, Boolean, Integer, Float, String, Numeric, \
    DateTime
from geoalchemy2 import Geometry
from shapely import wkt, wkb
from datetime import datetime
from ogr2ogr import main

from config import logger, DB_SCHEMA
from db import engine, TableName, Session, Localization, metadata, TableAlias

gis_import = Blueprint('gis_import', __name__)

GEOMETRY_TYPE = ['geometry', 'point', 'polygon', 'linestring', 'multilinestring', 'multipolygon',
                 'multipoint', 'polyhedralsurface', 'triangle', 'tin', 'geometrycollection']


def load_valid(geo, geometry_format='wkt'):
    try:
        return wkb.loads(geo, hex=True) if geometry_format == 'wkb' else wkt.loads(geo)
    except Exception:
        return np.nan


def check_encoding(file_path):
    with open(file_path, 'rb') as f:
        data = f.read(1000000)
    encoding = detect(data).get("encoding")
    return encoding


def parse_json(file_path, file_output, geometry_field, geometry_format):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        if geometry_field not in data[0]:
            return logger.error("not found geometry_field")
        geojson_data = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": d.pop(geometry_field) if not geometry_format or (geometry_format.lower() == 'geojson')
                else None,
                "properties": d if not d.get('properties') else d['properties'],
            } for d in data]
        }
    elif isinstance(data, dict) and data.get('features', None):
        geojson_data = data
    else:
        return logger.error("don't read json: not valid data")

    with open(file_output, 'w') as f:
        json.dump(geojson_data, f)


def check_columns(gdf, geometry_field):
    gdf = gdf.loc[:, ~gdf.columns.duplicated()].copy()
    for column in gdf.columns.tolist():
        if column == 'gis_id':
            del gdf[column]
        elif column == geometry_field:
            del gdf[column]
        elif column == 'geom':
            continue
        else:
            rep = re.compile("\W")
            new_column = rep.sub("_", column).lower()
            if column == new_column:
                continue
            elif new_column in gdf.columns.tolist():
                gdf.rename(columns={column: f"{new_column}_copy"}, inplace=True)
            else:
                gdf.rename(columns={column: new_column}, inplace=True)
    return gdf


def parser(file_path, table_name, file_type, geometry_field, geometry_format):
    """ Handling file. Convert file to dataframe, then upload to database.
        Entry data mush have file_path, table_name and file_type for upload table.
        Geometry_field and geometry_format need for correct processing of geometry"""

    file_output = os.path.join('files', f'out_{datetime.utcnow().strftime("%d_%m_%y_%H_%M_%S")}.geojson')

    if file_type == 'application/json':
        parse_json(file_path, file_output, geometry_field, geometry_format)
    elif file_type == 'application/zip':
        file_output = file_path
    else:
        main(["ogr2ogr", "-f", "GeoJSON", file_output, file_path])

    if not os.path.exists(file_output):
        return {"message": "failed to read file, not valid data"}, 400

    try:
        gdf = gpd.read_file(file_output).to_crs(epsg=4326)
    except UnicodeError:
        try:
            gdf = gpd.read_file(file_output, encoding='Windows-1251').to_crs(epsg=4326)
        except UnicodeError:
            encoding = check_encoding(file_path)
            if encoding:
                gdf = gpd.read_file(file_output, encoding=encoding).to_crs(epsg=4326)
            else:
                return {"message": "don't read encoding of file"}, 400
    except Exception as e:
        logger.error(e)
        return {"message": "failed convert to dataframe"}, 500

    # Adapt format of geometry in file
    if geometry_format == 'wkt':
        gdf.geometry = gdf[geometry_field].apply(load_valid) if geometry_field in gdf.columns.tolist() \
            else gdf.geometry.apply(load_valid)
    elif geometry_format == 'wkb':
        gdf.geometry = gdf[geometry_field].apply(load_valid,
                                                 geometry_format='wkb') if geometry_field in gdf.columns.tolist() \
            else gdf.geometry.apply(load_valid, geometry_format='wkb')
    elif geometry_format == 'xy':
        geometry_field = geometry_field.replace(' ', '').split(',')

        if len(geometry_field) != 2:
            return {"message": "not valid geometry_field for XY"}, 400
        if not set(geometry_field).issubset(set(gdf.columns.tolist())):
            return {"message": "not found columns XY"}, 400

        gdf.geometry = gpd.points_from_xy(gdf[geometry_field[0]], gdf[geometry_field[1]])

        gdf.drop(geometry_field, axis=1)

    gdf.rename_geometry('geom', inplace=True)

    gdf = check_columns(gdf, geometry_field)

    try:
        geometry_empty = gdf.geometry.isnull().all()
        if not geometry_empty:
            # Check valid geometry in table
            geometry_types = list(dict.fromkeys(gdf.geom_type.values))
            different_geometry = [geom for geom in geometry_types if str(geometry_types[0]) not in f'Multi{geom}']
            if different_geometry:
                return {"message": "different geometry"}, 400
            # Saving tables with geometry
            gdf.to_postgis(name=table_name, con=engine, schema=DB_SCHEMA, index=False,
                           if_exists='replace')
        else:
            # Saving tables without geometry
            gdf.to_sql(name=table_name, con=engine, schema=DB_SCHEMA, index=False,
                       if_exists='replace')
    except Exception as e:
        logger.error(e)
        return {"message": "don't put in postgis"}, 500

    return 'import done', 200


@gis_import.post('/gis/import')
def import_gis_file():
    """The function accepts GIS objects via POST request.
    Saves them in the database and leaves an entry in the registry.
    In response, the function returns the ID and alias of the table by which it can be accessed."""

    file_requested = request.files['file']
    geometry_field = request.form.get('geometry_field', 'geometry')
    geometry_format = request.form.get('geometry_format')
    alias = request.form.get('alias')
    language = request.form.get('locale', 'ru')

    if 'file' not in request.files:
        return jsonify({"message": "failed request"}), 400
    if file_requested.filename == '':
        return jsonify({"message": "not found file"}), 400

    table_name = f'geotable{datetime.utcnow().strftime("%d_%m_%y_%H_%M_%S")}'

    file_path = os.path.join('files', file_requested.filename)
    logger.info(f'{file_path=}')
    file_requested.save(file_path)

    message, status = parser(file_path, table_name, file_requested.content_type, geometry_field, geometry_format)

    if status != 200:
        return jsonify(message), status
    else:
        # Saving information to the registry of tables
        table = TableName(table_name=f'{DB_SCHEMA}.{table_name}')
        Session.add(table)
        Session.commit()

        # Saving alias to the registry of tables
        localization = Localization(language=language, alias=alias, table_id=table.id)
        Session.add(localization)
        Session.commit()

        # Add column gis_id as primary key
        query_gis_id = f"""ALTER TABLE {table.table_name} ADD COLUMN gis_id SERIAL PRIMARY KEY;"""
        Session.execute(query_gis_id)
        Session.commit()

        logger.info(message)
        return jsonify({"id": table.id, "alias": {language: alias}}), 201


@gis_import.post('/gis/import/empty')
def create_empty_gis():
    data = request.get_json()

    if not data:
        return jsonify({"message": "failed request"})

    geometry_type = data.get('geometry_type')
    if geometry_type:
        if geometry_type.lower() not in GEOMETRY_TYPE:
            return jsonify({"message": "failed geometry type"}), 400

    alias = data.get('alias') if data.get('alias') else None
    language = data.get('locale') if data.get('locale') else 'ru'

    fields = data.get('fields') if data.get('fields') else None

    columns = []
    if fields:
        for field in fields:
            if field.get('type') == 'boolean':
                column = Column(field.get('name'), Boolean)
                columns.append(column)
            elif field.get('type') == 'integer':
                column = Column(field.get('name'), Integer)
                columns.append(column)
            elif field.get('type') == 'numeric':
                column = Column(field.get('name'), Numeric)
                columns.append(column)
            elif field.get('type') == 'decimal':
                column = Column(field.get('name'), Float)
                columns.append(column)
            elif field.get('type') == 'string':
                column = Column(field.get('name'), String)
                columns.append(column)
            elif field.get('type') == 'time':
                column = Column(field.get('name'), DateTime)
                columns.append(column)
            else:
                return jsonify({"message": "not found type of attrs"}), 400

    table_name = f'geotable_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

    # Add column geom if geom doesn't exist
    if 'geom' not in data.keys() and geometry_type:
        columns.append(Column('geom', Geometry(geometry_type.upper())))

    table_obj = Table(
        table_name,
        metadata,
        Column('gis_id', Integer, primary_key=True),
        *columns
    )

    table_obj.create(engine)

    # Saving information to the registry of tables
    table = TableName(table_name=f'{DB_SCHEMA}.{table_name}')
    Session.add(table)
    Session.commit()

    # Saving alias to the registry of tables
    if alias:
        localization = Localization(language=language, alias=alias, table_id=table.id)
        Session.add(localization)
        Session.commit()

    for field in fields:
        if field.get('alias'):
            new_alias = TableAlias(
                table_id=table.id,
                language=language,
                table_field=field.get('name'),
                alias=field.get('alias'))
            Session.add(new_alias)
            Session.commit()

    return jsonify({'id': table.id, "alias": {language: alias}}), 201
