import json
import os
import sqlalchemy
from datetime import datetime
from pathlib import Path

from sqlalchemy import insert, Date
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import text, func

from flask import Blueprint, jsonify, request, send_file, send_from_directory
from db import json_query, get_table_class, TableName, Session, \
    CommentTable, Localization, TableFolder, TableFile

from utils import get_children, get_fields, get_geom_type, get_filter_set, add_filters


gis = Blueprint('gis', __name__)


@gis.get('/gis/tables')
def get_tables():
    """Returns JSON with a registry of tables"""
    tables = TableName.query.all()
    if tables is None:
        return jsonify({"message": "tables not found"}), 404

    data = {}

    as_array = False
    if 'as_array' in request.args:
        as_array = True

    for table in tables:
        locale = Localization.query.filter(Localization.table_id == table.id).all()
        alias_dict = {rec.language: rec.alias for rec in locale}
        Session.commit()

        if not table.is_folder and not table.parent_id:
            data[table.id] = {
                "id": table.id,
                "alias": alias_dict,
                "geom_type": get_geom_type(table.table_name),
                "parent_id": table.parent_id
            }
        if table.is_folder:
            data[table.id] = {
                "id": table.id,
                "alias": alias_dict,
                "children": get_children(table, as_array)
            }

    return jsonify({"count": len(data), "tables": list(data.values()) if as_array else data, "message": "success"})


@gis.post('/gis/<int:table_id>')
def get_table_id(table_id):
    """Displaying all table entries by ID"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404

    # getting a table description in all languages
    alias = Localization.query.filter(table.id == Localization.table_id).all()
    alias_dict = {rec.language: rec.alias for rec in alias} if alias else None

    as_array = True if 'as_array' in request.args else False

    if table.is_folder:
        return jsonify({"id": table.id, "alias": alias_dict,
                        "children": get_children(table, as_array)})

    try:
        table_obj = get_table_class(table.table_name)
    except NoSuchTableError:
        return jsonify({"message": "not found table in db"}), 502
    columns = table_obj.columns

    # Default Requests
    query = f"""SELECT * FROM {table.table_name}
                ORDER BY gis_id"""

    count_query = f"""SELECT COUNT(*) FROM {table.table_name}"""

    # The number of entries per page by default. Used for pagination.
    limit_value = 50

    if request.args:
        # Processing arguments for filtering and sorting.
        filter_set = get_filter_set(request.args.to_dict().items(), limit_value)
        query_set = filter_set.get('query_set')
        limit_value = filter_set.get('limit_value')
        limit = filter_set.get('limit')
        query_sort = filter_set.get('query_sort')

        if query_set:
            # Query with filters and sorting.
            query_args = f"""{' AND '.join(query_set)}
                    {query_sort} {limit}"""

            # Query is for displaying the number of pages.
            count_query = f"""SELECT COUNT(*) FROM {table.table_name} WHERE {' AND '.join(query_set)}"""
        else:
            # Query with pagination and sorting, but without additional filters.
            query_args = f"""{query_sort} {limit}"""

        query = f"""SELECT * FROM {table.table_name}
                        {query_args}"""
    else:
        query_args = None

    # Number of pages
    count_response = Session.execute(text(count_query)).first()[0]
    if count_response:
        if count_response % limit_value == 0:
            pages = count_response // limit_value
        else:
            pages = count_response // limit_value + 1
    else:
        pages = 0

    data = request.json
    if data:
        if data.get('attribute'):
            filters = add_filters(data.get('attribute'), columns)
            query = f"""SELECT * FROM {table.table_name}
                        WHERE {' AND '.join(filters) if filters else ''}
                        {query_args if query_args else ''}
                        """
        else:
            filters = None

        # JSON to Geometry conversions for SQL
        if data.get('spatial'):
            sql_geom = f"""ST_AsText(ST_GeomFromGeoJSON('{str(data.get('spatial')).replace("'", '"')}'))"""
            query = f"""SELECT * FROM {table.table_name}
                            WHERE ST_Intersects(geom, CONCAT('SRID=4326;', {sql_geom})::geometry)
                            {' AND '+' AND '.join(filters) if filters else ''}
                            {query_args if query_args else ''}
                            """

    try:
        response = Session.execute(json_query(query, as_array)).first()['data']
    except sqlalchemy.exc.InternalError as ex:
        # Returns an error, for example, if the SRID was incorrectly passed.
        return jsonify({"ERROR": str(ex).split('\n')[0]}), 503

    if response and ('geom' in columns):

        query_borders = f"""SELECT json_agg(f) FROM(SELECT
                            min(ST_XMin(data)) as "0",
                            min(ST_YMin(data)) as "1",
                            max(ST_XMax(data)) as "2",
                            max(ST_YMax(data)) as "3" FROM (SELECT box2d(geom)::geometry as data
                            FROM {table.table_name} ) gm) f"""

        response_borders = Session.execute(text(query_borders)).first()[0][0]
    else:
        response_borders = None

    return jsonify({"alias": alias_dict,
                    "data": response,
                    "parent_id": table.parent_id,
                    "pages": pages,
                    "borders": response_borders if response_borders else None})


@gis.put('/gis/<int:table_id>')
def create_gis_id(table_id):
    """Create new gis object"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    table_obj = get_table_class(table.table_name)
    fields = Session.execute(f"""SELECT json_agg(column_name) FROM information_schema.columns
                                 WHERE TABLE_NAME = '{table.table_name.split('.')[1]}'""").first()[0]

    data = request.get_json()

    if data is None:
        return jsonify({"message": "failed request"}), 400

    geom_type = get_geom_type(table.table_name)

    for key in data:
        if key not in fields:
            return jsonify({"message": f"failed request: not found field {key}"}), 400
        elif key == 'gis_id':
            return jsonify({"message": "failed request: gis_id already exists"}), 400

        if key == 'geom':
            if data[key].get('type') != geom_type:
                return jsonify({"message": "failed request: geom another type"}), 400

        if type(data[key]) is dict:
            data[key] = json.dumps(data[key])
        elif type(table_obj.columns[key].type) is Date:
            data[key] = f"to_timestamp({data[key]})"

    query = insert(table_obj).values(**data)
    res = Session.execute(query)
    Session.commit()
    return jsonify({"id": res.inserted_primary_key[0]}), 201


@gis.put('/gis/<int:table_id>/parent')
def update_parent_id(table_id):
    """Update parent_id of table"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    data = request.get_json()
    parent_id = data.get("parent_id")
    if not isinstance(parent_id, (int, type(None))):
        return jsonify({"message": "failed request"}), 400

    table_folder = TableName.query.filter(TableName.id == parent_id).first()
    folder = TableFolder.query.filter(TableFolder.name == table_folder.table_name).first() if table_folder else None
    if parent_id is None:
        Session.query(TableName).filter(TableName.id == table.id). \
            update({'parent_id': None}, synchronize_session=False)
        Session.commit()
        return jsonify({"message": f"table №{table_id} successfully removed from folder."}), 200
    if folder:
        Session.query(TableName).filter(TableName.id == table.id). \
            update({'parent_id': folder.id}, synchronize_session=False)
        Session.commit()
        return jsonify({"message": f"table №{table_id} successfully put in folder."}), 200

    return jsonify({"message": "not found parent_id"}), 400


@gis.delete('/gis/<int:table_id>')
def delete_table_id(table_id):
    """Deleting a table by ID"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:

        folder = TableFolder.query.filter(TableFolder.name == table.table_name).first()
        if folder is None:
            return jsonify({"message": "folder not found"}), 404

        for children in folder.children:
            Session.query(TableName).filter(TableName.id == children.id). \
                update({'parent_id': None}, synchronize_session=False)
            Session.commit()

        # Deleting a folder from registry
        Session.query(TableName).filter(TableName.table_name == folder.name).delete()
        Session.commit()

        # Deleting a folder
        Session.query(TableFolder).filter(TableFolder.id == folder.id).delete()
        Session.commit()

        return jsonify({"message": f"Successfully deleting folder №{folder.id}"})

    # Deleting files in table
    files = TableFile.query.filter(TableFile.table_id == table.id).all()
    for file in files:
        try:
            os.remove(file.path)
        except FileNotFoundError:
            continue

    # Deleting a table entry from the registry
    Session.delete(table)
    Session.commit()

    # Deleting the table itself or a record about it
    query = f'DROP TABLE {table.table_name};'
    Session.execute(query)
    Session.commit()
    return jsonify({"message": f"Table №{table_id} successfully deleted."})


@gis.get('/gis/<int:table_id>/<int:gis_id>')
def get_gis_id(table_id, gis_id):
    """Displaying table entries by ID"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    count_query = f"""SELECT array_agg(gis_id) FROM {table.table_name}"""

    count_response = Session.execute(text(count_query)).first()[0]

    if count_response is None:
        return jsonify({"message": "not found any records"}), 404

    field_base = Session.execute(f"""SELECT json_agg(column_name) FROM information_schema.columns
                                     WHERE TABLE_NAME = '{table.table_name.split('.')[1]}'""").first()[0]

    response_borders = None
    if gis_id in count_response:
        query = f"""SELECT json_agg(gis_obj)
                            FROM (SELECT * FROM {table.table_name} 
                            WHERE gis_id = {gis_id}) as gis_obj"""
        response = Session.execute(text(query)).first()[0][0]
        if 'geom' in field_base:
            query_borders = f"""SELECT json_agg(f) FROM(SELECT
                                                    min(ST_XMin(data)) as "0",
                                                    min(ST_YMin(data)) as "1",
                                                    max(ST_XMax(data)) as "2",
                                                    max(ST_YMax(data)) as "3" FROM (SELECT box2d(geom)::geometry as data
                                                    FROM {table.table_name}
                                                    WHERE gis_id = {gis_id}) as gm) f"""

            response_borders = Session.execute(text(query_borders)).first()[0][0]
    else:
        return jsonify({"message": "record not found"}), 404

    return jsonify({"data": response, "borders": response_borders if response_borders else None})


@gis.post('/gis/<int:table_id>/<int:gis_id>/copy')
def copy_gis_id(table_id, gis_id):
    """Copy record table by ID"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    table_obj = get_table_class(table.table_name)
    gis_obj = Session.query(*[col for col in table_obj.c if col.name != 'gis_id']). \
        filter(table_obj.c.gis_id == gis_id).first()
    if gis_obj:
        # Copy record in table
        query = insert(table_obj).values(**gis_obj)
        res = Session.execute(query)
        Session.commit()
    else:
        return jsonify({"message": "not found gis_id"}), 404

    return jsonify({"id": res.inserted_primary_key[0]}), 201


@gis.put('/gis/<int:table_id>/<int:gis_id>')
def put_gis_id(table_id, gis_id):
    """A request to change data in a table by ID receives a JSON"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    # JSON is accepted only with fields in db
    data = request.get_json()

    if not isinstance(data, dict):
        return jsonify({"message": "failed request"}), 400

    table_obj = get_table_class(table.table_name)

    for key, value in data.items():
        if not (key in table_obj.columns):
            return jsonify({"message": "failed request"}), 400
        if key == 'gis_id':
            del data['gis_id']
        elif key == 'geom':
            data["geom"] = func.ST_AsText(func.ST_GeomFromGeoJSON(str(data.get('geom')).replace("'", '"')))

    query = table_obj.update().values(data).where(table_obj.c.gis_id == gis_id)

    Session.execute(query)
    Session.commit()

    return jsonify({"id": gis_id}), 200


@gis.delete('/gis/<int:table_id>/<int:gis_id>')
def delete_gis_id(table_id, gis_id):
    """Deleting a record in table by ID"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    table_obj = get_table_class(table.table_name)
    gis_obj = Session.query(table_obj).filter(table_obj.c.gis_id == gis_id).first()

    files = TableFile.query.filter(TableFile.table_id == table_id, TableFile.row_id == gis_id).all()
    # Checking that a record with this id exists
    if gis_obj:
        if files:
            # Deleting file in record
            for file in files:
                try:
                    os.remove(file.path)
                except FileNotFoundError:
                    continue
            # Deleting record about file from table files
            TableFile.query.filter(TableFile.table_id == table_id, TableFile.row_id == gis_id).delete()

        # Deleting comments in record
        CommentTable.query.filter(CommentTable.table_id == table_id,
                                  CommentTable.row_id == gis_id).delete()

        query = f'DELETE FROM {table.table_name} WHERE gis_id = {gis_id};'
    else:
        return jsonify({"message": "Row not found"}), 404

    # Deleting the table itself or a record about it
    Session.execute(query)
    Session.commit()

    return jsonify({"message": f"Row № {gis_id} successfully deleted."})


@gis.post('/gis/folders')
def create_folder():
    """Create a new folder"""

    alias = request.form.get('alias')

    if alias is None:
        return jsonify({"message": "not found data"}), 400

    name = f'folder{datetime.utcnow().strftime("%d_%m_%y_%H_%M_%S")}'

    # Saving folder to the table of folders
    folder = TableFolder(name=name)

    # Saving folder to the registry of tables
    table = TableName(table_name=f'{folder.name}', is_folder=True)
    Session.add_all([folder, table])
    Session.commit()

    # Saving alias to the registry of tables
    localization = Localization(language='ru', alias=alias, table_id=table.id)
    Session.add(localization)
    Session.commit()

    return jsonify({"id": table.id}), 201


@gis.get('/gis/<int:table_id>/export')
def export_data(table_id):
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    location = os.path.join('gis_export_files')
    Path(location).mkdir(parents=True, exist_ok=True)

    filename = f'file_geotable_{datetime.utcnow().strftime("%d_%m_%y_%H_%M_%S")}'
    path_file = f'{filename}.geojson'
    path = os.path.join(location, path_file)

    query = f""" SELECT json_agg(f) FROM (SELECT * FROM {table.table_name}) f """
    data = Session.execute(text(query)).first()[0]
    Session.commit()

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": d.pop('geom', None),
                "properties": d,
            } for d in data]
    }

    with open(path, 'w') as f:
        json.dump(geojson, f)

    return send_from_directory(directory=location, path=path_file,
                               as_attachment=True, download_name=filename)
