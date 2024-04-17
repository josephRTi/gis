from flask import Blueprint, jsonify

endpoints = Blueprint('endpoints', __name__)


@endpoints.get('/gis/endpoints')
def get_endpoints():
    module_info = {
        'name': 'gis',
        'alias': 'ГИС',
        'endpoints': [
            {
                'path': '/import',
                'description': 'Менеджер источников данных',
                'get': False,
                'post': True,
                'put': False,
                'delete': False
            },
            {
                'path': '/import/empty',
                'description': 'Создание пустого ИД',
                'get': False,
                'post': True,
                'put': False,
                'delete': False
            },
            {
                'path': '/tables',
                'description': 'Получение реестра таблиц ГИС с описанием полей',
                'get': True,
                'post': False,
                'put': False,
                'delete': False
            },
            {
                'path': '/<int>',
                'description': 'Работа с ГИС таблицами',
                'get': False,
                'post': True,
                'put': True,
                'delete': True
            },
            {
                'path': '/<int>/export',
                'description': 'Экспорт ГИС таблицы в файл .geojson',
                'get': True,
                'post': False,
                'put': False,
                'delete': False
            },
            {
                'path': '/<int>/parent',
                'description': 'Вложить таблицу ГИС в папку. ',
                'get': False,
                'post': False,
                'put': True,
                'delete': False
            },
            {
                'path': '/<int>/<int>',
                'description': 'Работа с объектами ГИС таблицы',
                'get': True,
                'post': False,
                'put': True,
                'delete': True
            },
            {
                'path': '/<int>/<int>/copy',
                'description': 'Копирование объекта из ГИС таблицы. ',
                'get': False,
                'post': True,
                'put': False,
                'delete': False
            },
            {
                'path': '/folders',
                'description': 'Работа с таблицами вложенности ГИС',
                'get': False,
                'post': True,
                'put': False,
                'delete': False
            },
            {
                'path': '/<int>/documents',
                'description': 'Работа с файлами для записей в ГИС таблицах',
                'get': True,
                'post': True,
                'put': False,
                'delete': False
            },
            {
                'path': '/<int>/documents/<int>/download',
                'description': 'Получение файлов из ГИС таблиц',
                'get': True,
                'post': False,
                'put': False,
                'delete': False
            },
            {
                'path': '/<int>/documents/<int>',
                'description': 'Удаление файлов из записей ГИС таблицы',
                'get': False,
                'post': False,
                'put': False,
                'delete': True
            },
            {
                'path': '/<int>/comments',
                'description': 'Работа с комментариями в записях ГИС таблицы по table_id',
                'get': True,
                'post': True,
                'put': False,
                'delete': False
            },
            {
                'path': '/<int>/comments/<int>',
                'description': 'Удаление комментариев из записей ГИС таблицы',
                'get': False,
                'post': False,
                'put': False,
                'delete': True
            },
            {
                'path': '/localization/<int>',
                'description': 'Работа с локализацией в описании ГИС таблицы по ID',
                'get': False,
                'post': False,
                'put': True,
                'delete': True
            },
            {
                'path': '/fields',
                'description': 'Получить все типы полей для добавления новых столбцов в ГИС таблице',
                'get': True,
                'post': False,
                'put': False,
                'delete': False
            },
            {
                'path': '/fields/<int>',
                'description': 'Работа с типами полей и их описаниями ГИС таблиц по ID',
                'get': True,
                'post': False,
                'put': True,
                'delete': False
            },
            {
                'path': '/img',
                'description': 'Работа с изображениями',
                'get': True,
                'post': True,
                'put': False,
                'delete': False
            },
        ]
    }
    return jsonify(module_info)
