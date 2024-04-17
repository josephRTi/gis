import os
from pathlib import Path
from datetime import datetime

from flask import Blueprint, jsonify, request, send_from_directory
from db import get_table_class, TableName, Session, TableFile
from werkzeug.utils import secure_filename

documents = Blueprint('documents', __name__)


@documents.get('/gis/<int:table_id>/documents')
def get_documents(table_id):
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    gis_id = request.args.get('gis_id') if request.args else None

    if gis_id:
        docs = TableFile.query.filter(TableFile.table_id == table_id, TableFile.row_id == gis_id).all()
    else:
        docs = TableFile.query.filter_by(table_id=table_id).all()

    res = {document.id: {
        "id": document.id,
        "path": document.path,
        "name": document.name,
        "gis_id": document.row_id,
        "created_at": f"{document.created_at:%Y-%m-%dT%H:%M:%S%z}" if document.created_at else None,
        "created_by": document.created_by
    } for document in docs}

    if 'as_array' in request.args:
        return jsonify(list(res.values()))
    return jsonify(res)


@documents.post('/gis/<int:table_id>/documents')
def upload_file_for_table(table_id):
    """The function accepts a file via form_data for uploading to records in tables."""

    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    # Rename file name and create path
    file = request.files['file']
    if 'file' not in request.files:
        return jsonify({"message": "failed request"}), 400
    if file.filename == '':
        return jsonify({"message": "not found file"}), 400

    gis_id = request.form.get('gis_id')
    if gis_id is None:
        return jsonify({"message": "failed request"}), 400

    try:
        gis_id = int(gis_id)
    except ValueError:
        return jsonify({"message": "failed request"}), 400

    table_obj = get_table_class(table.table_name)
    if not Session.query(table_obj).filter(table_obj.c.gis_id == gis_id).first():
        return jsonify({"message": "failed request"}), 400

    file_name = str(datetime.now().timestamp()).replace('.', '_') + '.' + file.filename.split('.')[-1]

    location = os.path.join('gis_upload_files')
    Path(location).mkdir(parents=True, exist_ok=True)
    path = os.path.join(location, file_name)

    file_obj = TableFile.query.filter(TableFile.path == path, TableFile.row_id == gis_id).first()

    if file_obj:
        # Delete the old file if it was linked to the record
        try:
            os.remove(path)
        except FileNotFoundError:
            return jsonify({"message": "not found file in directory for update"}), 503
    else:
        record_file = TableFile(table_id=table.id, row_id=gis_id, path=path,
                                name=secure_filename(file.filename),
                                filename=file_name, created_by=request.form.get('created_by', None))
        Session.add(record_file)
        Session.commit()

    # We save a new file, rename it by linking the number of the table and record, and also use transliteration
    file.save(path)

    return jsonify({"id": record_file.id if record_file else file_obj.id,
                    "path": path}), 200


@documents.get('/gis/<int:table_id>/documents/<int:file_id>/download')
def get_download_file(table_id, file_id):
    """To upload a file in the argument, you need to submit id of download_file """
    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    doc = TableFile.query.filter(TableFile.id == file_id, TableFile.table_id == table_id).first()
    if doc is None:
        return jsonify({"message": "file not found"}), 404
    if doc.path and os.path.isfile(doc.path):
        return send_from_directory(directory=os.path.dirname(doc.path), path=os.path.basename(doc.path),
                                   as_attachment=True, download_name=doc.name)
    else:
        Session.delete(doc)
        Session.commit()
    return jsonify({"message": "file not found"}), 404


@documents.delete('/gis/<int:table_id>/documents/<int:file_id>')
def delete_documents(table_id, file_id):
    """The function deletes a doc to an entry in the table by the doc's ID"""
    doc = TableFile.query.filter(TableFile.id == file_id, TableFile.table_id == table_id).first()
    if doc is None:
        return jsonify({"message": "file not found"}), 404
    try:
        os.remove(doc.path)
    except FileNotFoundError:
        pass
    Session.query(TableFile).filter(TableFile.id == file_id, TableFile.table_id == table_id).delete()
    Session.commit()
    return jsonify({"id": doc.id, "message": "Successfully deleted file"})
