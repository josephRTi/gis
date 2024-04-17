from db import get_table_class, CommentTable, TableName, Session

from flask import Blueprint, jsonify, request

comments = Blueprint('comments', __name__)


@comments.get('/gis/<int:table_id>/comments')
def get_comments(table_id):
    """The function returns all comments on the table ID and the record ID in the table"""
    table = TableName.query.filter(TableName.id == table_id).first()
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    gis_id = request.args.get('gis_id') if request.args else None
    if gis_id:
        comments_lst = CommentTable.query.filter(CommentTable.table_id == table_id, CommentTable.row_id == gis_id).all()
    else:
        comments_lst = CommentTable.query.filter_by(table_id=table_id).all()

    res = {
        comment.id: {
            "id": comment.id,
            "gis_id": comment.row_id,
            "created_at": f"{comment.created_at:%Y-%m-%dT%H:%M:%S%z}" if comment.created_at else None,
            "created_by": comment.created_by,
            "text": comment.text
        } for comment in comments_lst}

    if 'as_array' in request.args:
        return jsonify(list(res.values()))
    return jsonify(res)


@comments.post('/gis/<int:table_id>/comments')
def create_comment(table_id):
    """The function creates a comment to an entry in the table by table ID and record ID"""

    table = TableName.query.get(table_id)
    if table is None:
        return jsonify({"message": "table not found"}), 404
    if table.is_folder:
        return jsonify({"message": "folder does not have this method"}), 405

    data = request.get_json()
    if data is None:
        return jsonify({"message": "Failed request"}), 400

    table_obj = get_table_class(table.table_name)

    # Checking that the data has gis_id
    gis_id = data.get('gis_id')
    if gis_id is None:
        return jsonify({"message": "Failed request"}), 400

    # Checking that the record to which the comment will be added exists
    if not Session.query(table_obj).filter(table_obj.c.gis_id == gis_id).first():
        return jsonify({"message": "Row not found"}), 404

    try:
        comment = CommentTable(table_id=table_id,
                               row_id=gis_id,
                               created_by=data.get('created_by', None),
                               text=data.get('text'))
    except KeyError:
        return jsonify({"message": "Failed request"}), 400

    Session.add(comment)
    Session.commit()

    return jsonify({'id': comment.id}), 201


@comments.delete('/gis/<int:table_id>/comments/<int:comment_id>')
def delete_comment(table_id, comment_id):
    """The function deletes a comment to an entry in the table by the comment ID"""

    comment = CommentTable.query.filter(CommentTable.table_id == table_id, CommentTable.id == comment_id).first()

    if comment is None:
        return jsonify({"message": "comment not found"}), 404

    Session.query(CommentTable).filter(CommentTable.table_id == table_id, CommentTable.id == comment_id).delete()
    Session.commit()

    return jsonify({"id": comment.id, "message": "Successfully deleted comment"})

