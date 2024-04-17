import re
from flask import Blueprint, jsonify, request

from db import TableName, Localization, Session, TableAlias, get_table_class
from blueprints.gis import get_geom_type

localization = Blueprint('localization', __name__)


@localization.put('/gis/localization/<int:table_id>')
def update_alias(table_id):
    """The function updated localization in table"""

    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404

    data = request.get_json()
    if data is None:
        return jsonify({"message": "Failed request"}), 400
    for keys, values in data.items():
        alias_old = Localization.query.filter(Localization.table_id == table_id,
                                              Localization.language == keys).first()
        if alias_old:
            alias_old.alias = values
            Session.commit()
        else:
            alias_new = Localization(table_id=table_id, language=keys, alias=values)
            Session.add(alias_new)
            Session.commit()
    return jsonify({'message': f'Alias table №{table_id} update!'})


@localization.delete('/gis/localization/<int:table_id>')
def delete_alias(table_id):
    """The function removes localization"""

    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404

    language = request.args.get('del')
    alias_delete = Localization.query.filter(Localization.table_id == table_id,
                                             Localization.language == language).first()
    if alias_delete:
        Session.delete(alias_delete)
        Session.commit()
        return jsonify({'message': f'Localization "{language}" in table №{table_id} delete!'})
    else:
        return jsonify({"message": "Localization not found"}), 404


@localization.get('/gis/fields/<int:table_id>')
def get_fileds(table_id):
    """getting table field types in a separate method
    and converting to a single view for frontend and
    getting alias for field"""
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    # getting table types from the database
    fields_base = Session.execute(f"""SELECT column_name, udt_name 
                                      FROM information_schema.columns
                                      WHERE TABLE_NAME = '{table.table_name.split('.')[1]}'""").all()

    # beauty guidance for frontend
    fields_dict = {}
    for field in fields_base:
        if 'json' in field[1]:
            fields_dict[field[0]] = {'type': 'json'}
        elif 'float' in field[1] or 'numeric' in field[1]:
            fields_dict[field[0]] = {'type': 'numeric'}
        elif 'int' in field[1]:
            query = f"""SELECT MIN("{field[0]}"), MAX("{field[0]}") FROM {table.table_name}"""
            field_extremes = Session.execute(query).first()

            fields_dict[field[0]] = {'type': 'integer',
                                     'min': field_extremes[0],
                                     'max': field_extremes[1]}
        elif field[1] == 'date':
            fields_dict[field[0]] = {'type': 'date'}
        elif 'time' in field[1]:
            fields_dict[field[0]] = {'type': 'datetime'}
        elif 'geom' in field[1]:
            fields_dict[field[0]] = {'type': 'geometry',
                                     'geometry_type': get_geom_type(table.table_name)}
        elif 'bool' in field[1]:
            fields_dict[field[0]] = {'type': 'boolean'}
        else:
            fields_dict[field[0]] = {'type': 'string'}

        alias = TableAlias.query.filter(TableAlias.table_id == table_id, TableAlias.table_field == field[0]).all()
        if alias:
            alias_dict = {rec.language: rec.alias for rec in alias}
            fields_dict[field[0]] |= {"alias": alias_dict}
        else:
            fields_dict[field[0]] |= {"alias": None}

        if 'float' in field[1] or 'numeric' in field[1] or 'int' in field[1]:
            query = f"""SELECT MIN("{field[0]}"), MAX("{field[0]}") FROM {table.table_name}"""
            field_extremes = Session.execute(query).first()

            fields_dict[field[0]].update({'min': field_extremes[0], 'max': field_extremes[1]})

    return jsonify(fields_dict)


@localization.get('/gis/fields')
def get_attrs():
    """the function returns the possible types of fields to add"""
    return jsonify({
        "string": "строчный тип данных",
        "time": "время с учётом часового пояса",
        "integer": "целое число",
        "boolean": "логическое значение",
        "numeric": "вещественное число"
    })


@localization.put('/gis/fields/<int:table_id>')
def new_attr(table_id):
    """Accepts json with the name and type of the new field in the table"""

    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    data = request.get_json()
    if not data:
        return jsonify({"message": "failed request"}), 400
    if not isinstance(data, list):
        return jsonify({"message": "failed request"}), 400

    field_base = Session.execute(f"""SELECT json_agg(column_name)
                                     FROM information_schema.columns
                                     WHERE TABLE_NAME = '{table.table_name.split('.')[1]}'""").first()[0]

    possible_attributes = {"string", "time", "integer", "boolean", "numeric"}

    for field in data:

        field_type = field.get('type') if field.get('type') != 'string' else 'text'

        field_name = field.get('name', None)
        if not field_name:
            return jsonify({"message": "failed request"}), 400

        rep = re.compile("\W")
        field_name = rep.sub("_", field_name).lower()

        if field_name == 'gis_id':
            continue
        if field_name in field_base:
            if field.get('disabled'):

                query = f"""ALTER TABLE {table.table_name} DROP COLUMN {field_name}"""
                Session.execute(query)
                Session.commit()

                Session.query(TableAlias).filter(TableAlias.table_id == table_id,
                                                 TableAlias.table_field == field_name,
                                                 TableAlias.language == field.get('locale', 'ru')).delete()
                Session.commit()
            else:
                if 'alias' in field.keys():
                    alias_old = TableAlias.query.filter(TableAlias.table_id == table_id,
                                                        TableAlias.table_field == field_name,
                                                        TableAlias.language == field.get('locale', 'ru')).first()
                    if alias_old:
                        alias_old.alias = str(field.get('alias'))
                    else:
                        alias = TableAlias(language=field.get('locale', 'ru'), alias=str(field.get('alias')),
                                           table_id=table.id, table_field=field_name)
                        Session.add(alias)
                    Session.commit()
        else:

            if field.get('type') not in possible_attributes:
                return jsonify({"message": "unknown field type"}), 503
            query = f"""ALTER TABLE {table.table_name} ADD COLUMN {field_name} {field_type
            if field_type != 'time' else 'timestamp'};"""
            Session.execute(query)
            Session.commit()
            if field.get('alias'):
                locale = TableAlias(language=field.get('locale', 'ru'), alias=field.get('alias'),
                                    table_id=table.id, table_field=field_name)
                Session.add(locale)
                Session.commit()

    return jsonify({'id': table.id}), 201
