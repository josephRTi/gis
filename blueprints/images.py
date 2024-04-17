import os
from datetime import datetime

from flask import Blueprint, request, send_file
from PIL import Image, UnidentifiedImageError

images = Blueprint('images', __name__)


@images.post('/gis/img')
def post_img():
    file = request.files.get('file')
    if not file or file.mimetype != 'image/jpeg':
        return 'image/jpeg mimetype required'
    filename_ = f'screenshot_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}'
    path = os.path.join('files', f'{filename_}.jpg')
    file.save(path)
    filenames = {'jpg': f'{filename_}.jpg'}
    fmt = request.form.get('format')
    if not fmt:
        return filenames
    formats = ['tiff', 'png', 'gif', 'bmp']
    if fmt not in formats:
        return "Incorrect file type. 'tiff', 'png', 'gif' or 'bmp' required"
    try:
        im = Image.open(path)
    except UnidentifiedImageError:
        return 'UnidentifiedImageError'
    filenames[fmt] = f"{filename_}.{fmt}"
    im.save(os.path.join("files", filenames[fmt]), fmt)
    return filenames


@images.get('/gis/img')
def get_img():
    filename = request.args['filename']
    return send_file(
        os.path.join("files", filename),
        as_attachment=True,
        download_name=filename)
