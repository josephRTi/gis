import pytest
from db import Session, TableName, engine
import geopandas as gpd
import pandas as pd
from app import app


@pytest.fixture
def test_app():
    test_app = app
    yield test_app


@pytest.fixture()
def client(test_app):
    return test_app.test_client()


def get_table_fields(gdf):
    # Getting all fields to write to the table registry
    import_fields = gdf.columns.values.tolist()
    import_fields_type = [str(i) for i in gdf.dtypes.values.tolist()]
    fields = {import_fields[i]: import_fields_type[i] for i in range(len(import_fields))}
    fields["gis_ksodd_id"] = "int64"
    return fields


@pytest.fixture()
def new_tables():
    alias_1 = "Тестовая ГИС таблица № 1. Аэропорты"
    alias_2 = "Тестовая ГИС таблица № 2. Светофоры"
    alias_3 = "Тестовая КСОДД таблица № 1. Линии"

    gdf_1 = gpd.read_file("tests/data/airports.geojson")
    gdf_2 = gpd.read_file("tests/data/svet_tagil.zip")
    gdf_3 = gpd.read_file("tests/data/lines(KSODD).geojson")

    # Date conversion for KSODD objects
    gdf_3.rename(columns={'date': 'ksodd_date'}, inplace=True)
    gdf_3['ksodd_date'] = pd.to_datetime(gdf_3['ksodd_date']).dt.date

    fields_1 = get_table_fields(gdf_1)
    fields_2 = get_table_fields(gdf_2)
    fields_3 = get_table_fields(gdf_3)

    table_name_1 = "test_table_1"
    table_name_2 = "test_table_2"
    table_name_3 = "test_table_3"

    # Saving tables with geometry. From GEOPandas tables.
    gdf_1.to_postgis(name=table_name_1, con=engine, schema="public", index_label='gis_ksodd_id', index=True)
    gdf_2.to_postgis(name=table_name_2, con=engine, schema="public", index_label='gis_ksodd_id', index=True)
    gdf_3.to_postgis(name=table_name_3, con=engine, schema="public", index_label='gis_ksodd_id', index=True)

    # Saving information to the registry of tables
    table_1 = TableName(table_name=f'public."{table_name_1}"', alias=alias_1, fields=fields_1, is_ksodd=False)
    table_2 = TableName(table_name=f'public."{table_name_2}"', alias=alias_2, fields=fields_2, is_ksodd=False)
    table_3 = TableName(table_name=f'public."{table_name_3}"', alias=alias_3, fields=fields_3, is_ksodd=True)
    Session.add(table_1)
    Session.add(table_2)
    Session.add(table_3)

    Session.commit()
    list_tables = [table_1.id, table_2.id, table_3.id]
    yield list_tables

    # Deleting a table entry from the registry
    Session.delete(table_1)
    Session.delete(table_2)
    Session.delete(table_3)
    Session.commit()
    # Deleting the tables
    Session.execute(f'DROP TABLE {table_1.table_name};')
    Session.execute(f'DROP TABLE {table_2.table_name};')
    Session.execute(f'DROP TABLE {table_3.table_name};')
    Session.commit()
