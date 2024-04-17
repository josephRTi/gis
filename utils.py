import sqlalchemy
from db import Session, TableFolder, Localization


GEOM_TYPE = ['Geometry', 'Point', 'Polygon', 'LineString', 'MultiLineString', 'MultiPolygon',
             'MultiPoint', 'PolyhedralSurface', 'Triangle', 'Tin', 'GeometryCollection']


def get_filter_set(request_arg, limit_value):
    """processing filters for a GET request"""
    query_set = []
    query_sort = "ORDER BY gis_id"
    limit = ''
    as_array = False
    for keys, values in request_arg:
        # Sorting by a specific field. The "-" in the argument is responsible for reverse sorting
        if keys == 'sortby':
            if values.startswith('-'):
                values = f'"{values[1:].strip()}" DESC'
            else:
                values = f'"{values.strip()}"'
            query_sort = f'ORDER BY {values}, gis_id'

        # Search by mask. Analog of LIKE from SQL
        elif keys == 'mask':
            field, mask = values.split('=')
            condition = f'"{field}" LIKE \'{mask}\''
            query_set.append(condition)

        # Arguments for pagination
        elif keys == 'limit':
            limit_value = int(values)
        elif keys == 'page':
            page = int(values)
            limit = f"LIMIT {limit_value} OFFSET {limit_value * (page - 1)}"

        elif keys == 'as_array':
            as_array = True

    result = {
        "query_set": query_set,
        "limit_value": limit_value,
        "limit": limit,
        "query_sort": query_sort,
        "as_array": as_array
    }
    return result


def get_fields(fields):
    res = {}
    for field in fields:
        if str(field.type) == 'INTEGER' or str(field.type) == 'SMALLINT' or str(field.type) == 'FLOAT':
            field_type = 'num'
        else:
            field_type = 'str'
        res.update({field.description: field_type})
    return res


def add_filters(args, fields):
    res = []
    if not args:
        return res
    table_fields = get_fields(fields)
    for attr in args:
        if attr['field'] in table_fields:
            if attr['op'] == '==':
                attr['op'] = '='
            if table_fields[attr['field']] == 'num':
                res.append(f"{attr['field']} {attr['op']} {attr['value']}")
            else:
                res.append(f"{attr['field']} {attr['op']} '{attr['value']}'")
    return res


def get_geom_type(table_name):
    try:
        geom_type = Session.execute(f"""SELECT type::text
                                        FROM geometry_columns 
                                        WHERE f_table_schema = 'public' 
                                        AND f_table_name = 
                                        '{table_name.split('.')[1] if '.' in table_name else table_name}' 
                                        and f_geometry_column = 'geom'""").first()
    except sqlalchemy.exc.ProgrammingError:
        geom_type = None
    if geom_type:
        geom_type = geom_type[0] if geom_type[0] else None
        if geom_type == 'GEOMETRY':
            try:
                geom_obj = Session.execute(f"""SELECT json(geom) FROM {table_name} LIMIT 1;""").first()
            except sqlalchemy.exc.ProgrammingError:
                geom_obj = None
            if geom_obj:
                geom_type = geom_obj[0].get('type') if geom_obj[0] else None
                if str(geom_type).startswith('Multi'):
                    geom_type = geom_type.replace('Multi', '')
        else:
            for type_geom in GEOM_TYPE:
                if geom_type == type_geom.upper():
                    geom_type = type_geom

    return geom_type


def get_children(table, as_array=False):
    folder = TableFolder.query.filter(TableFolder.name == table.table_name).first()
    if folder:
        children = {
            child.id: {
                "id": child.id,
                "alias": {rec.language: rec.alias
                          for rec in Localization.query.filter(child.id == Localization.table_id)},
                "parent_id": table.id,
                "geom_type": get_geom_type(child.table_name)
            } for child in folder.children
        }
        output_data = list(children.values()) if as_array else children
    else:
        output_data = None
    return output_data

