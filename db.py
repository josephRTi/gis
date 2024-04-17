import logging
from config import DB_URL, DB_SCHEMA
from sqlalchemy import Table, Column, Integer, String, create_engine, MetaData, Boolean, ForeignKey, DateTime, Text
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.orm import sessionmaker, scoped_session, relationship

logger = logging.getLogger('db')

engine = create_engine(DB_URL, pool_pre_ping=True)
metadata = MetaData(bind=engine, schema=DB_SCHEMA)
Session = scoped_session(sessionmaker(bind=engine))


@as_declarative(metadata=metadata)
class Base:
    query = Session.query_property()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, comment='ID таблицы')
    name = Column(String, comment='Имя')
    username = Column(String, comment='Имя пользователя')
    hashed_pwd = Column(String, comment='Пароль', default=False)
    organization_id = Column(Integer, ForeignKey('organizations.id'), comment='ID организации')
    role_id = Column(Integer, ForeignKey('roles.id'), comment='ID роли')


class Role(Base):
    __tablename__ = 'roles'
    id = Column(Integer, primary_key=True, comment='ID таблицы')
    name = Column(String, comment='Имя')
    organization_id = Column(Integer, ForeignKey('organizations.id'), comment='ID организации')
    get = Column(Boolean, comment='GET', default=False)
    put = Column(Boolean, comment='PUT', default=False)
    post = Column(Boolean, comment='POST', default=False)
    delete = Column(Boolean, comment='DELETE', default=False)


class Organization(Base):
    __tablename__ = 'organizations'
    id = Column(Integer, primary_key=True, comment='ID таблицы')
    name = Column(String, comment='Имя')


class TableName(Base):
    __tablename__ = 'table_names'
    id = Column(Integer, primary_key=True, comment='ID таблицы')
    table_name = Column(String, comment='Название таблицы')
    is_folder = Column(Boolean, comment='Является ли таблица папкой', default=False)
    parent_id = Column(Integer, ForeignKey('table_folders.id'), comment='ID таблицы вложенности')


class CommentTable(Base):
    __tablename__ = 'comments_tables'

    id = Column(Integer, primary_key=True, comment='ID комментария')
    table_id = Column(Integer, ForeignKey(TableName.id, ondelete='CASCADE'), comment='ID ГИС таблицы', nullable=False)
    row_id = Column(Integer, comment='ID записи в ГИС таблице(gis_id)')
    created_by = Column(String, comment='Имя пользователя, оставившего комментарий')
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment='Дата создания')
    text = Column(Text, comment='Текст комментария')


class Localization(Base):
    __tablename__ = 'localization'
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey(TableName.id, ondelete='CASCADE'), comment='ID таблицы', nullable=False)
    language = Column(String, comment="Код языка")
    alias = Column(String, comment='Псевдоним таблицы')


class TableFolder(Base):
    __tablename__ = 'table_folders'
    id = Column(Integer, primary_key=True, comment='ID таблицы вложенности')
    name = Column(String, comment='Наименование таблицы вложеннности')
    children = relationship(TableName, backref='table_folders')


class TableAlias(Base):
    __tablename__ = 'table_aliases'
    id = Column(Integer, primary_key=True, comment='ID cоответствия')
    table_id = Column(Integer, ForeignKey(TableName.id, ondelete='CASCADE'), comment='ID таблицы', nullable=False)
    language = Column(String, comment="Код языка")
    table_field = Column(String, comment='Наименование поля таблицы')
    alias = Column(String, comment='Наименование описания поля таблицы')


class TableFile(Base):
    __tablename__ = 'table_files'

    id = Column(Integer, primary_key=True, comment='ID файла')
    table_id = Column(Integer, ForeignKey(TableName.id, ondelete='CASCADE'),
                      comment='ID ГИС таблицы', nullable=False)
    row_id = Column(Integer, comment='ID записи в ГИС таблице(gis_id)')
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment='Дата создания файла')
    created_by = Column(String, comment='Автор создания файла')
    filename = Column(String, comment='Имя файла в директории')
    name = Column(String, comment='Имя файла для юзера в системе')
    path = Column(String, comment='Путь к файлу')


def init_db():
    metadata.create_all()
    logger.info('Tables created')


def get_table_class(table_name):
    if '.' in table_name:
        table_name = table_name.split(".")[1]
    res = metadata.tables.get(table_name)
    if res is None:
        res = Table(table_name, metadata, autoload_with=engine)
    return res


def json_query(query, as_array=False):
    """
    Описание: функция для преобразования запроса в запрос, возвращающий json
    query - строковый запрос (без каких либо форматирований в json)
    as_array (умол: False) - отвечает за формат выходного json ([{}, {}] или [id1: {}, id2: {}])
    Возвращает: строку с модифицированным запросом
    """
    if as_array:
        return f"WITH query as ({query}) SELECT json_agg(query) as data FROM query;"
    else:
        return f"WITH query as ({query}) " \
               f"SELECT COALESCE(json_object_agg(query.gis_id, " \
               "row_to_json(query)), '{}'::json) as data FROM query; "
