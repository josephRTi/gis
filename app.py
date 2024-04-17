from flask import Flask, request
from waitress import serve

from blueprints import endpoints, gis, comments, documents, localization, \
    gis_import, images
from config import HOST, PORT, logger
from db import Session, init_db

app = Flask(__name__)
app.teardown_request(lambda *args: Session.remove())  # There was a problem with closing sessions
app.config['JSON_AS_ASCII'] = False

app.register_blueprint(endpoints)
app.register_blueprint(gis)
app.register_blueprint(comments)
app.register_blueprint(documents)
app.register_blueprint(gis_import)
app.register_blueprint(images)
app.register_blueprint(localization)


@app.after_request
def after_request(response):
    log = logger.info if response.status_code in (200, 201) else logger.error
    log('%s %s %s %s %s', request.remote_addr, request.method, request.scheme, request.full_path,
        response.status)
    return response


if __name__ == '__main__':
    init_db()
    serve(app, host=HOST, port=PORT, threads=10)
