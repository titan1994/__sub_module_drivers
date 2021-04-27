"""
Драйвер кликхауса
https://github.com/long2ice/asynch
"""
import asynch
from asynch.cursors import DictCursor
from clickhouse_driver import connect as SyncConnect

from pathlib import Path
from MODS.scripts.python.jinja import jinja_render_to_str, jinja_render_str_to_str
from ..metadata import ycl_ycl_convertations
from GENERAL_CONFIG import GeneralConfig

DEFAULT_JINJA_PATTERN_META = Path(__file__).parent / 'get_meta.jinja'
DEFAULT_JINJA_PATTERN_CREATE_TABLE = Path(__file__).parent / 'create_table.jinja'
DEFAULT_JINJA_PATTERN_CREATE_DICT = Path(__file__).parent / 'create_dict.jinja'
DEFAULT_JINJA_PATTERN_CREATE_MVIEW = Path(__file__).parent / 'create_mview.jinja'

"""
Обобщённый функционал
"""


class YandexConnector:
    """
    Асинхронный драйвер коннектора
    """

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    async def __aenter__(self):
        self.conn = await asynch.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.conn.close()


class YandexSyncConnector:
    """
    Синхронный драйвер коннектора
    """

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def __enter__(self):
        self.conn = SyncConnect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


async def do_execute(host, port, user, password, database, text_req):
    """
    Проброс execute
    :param host:
    :param port:
    :param user:
    :param password:
    :param database:
    :param text_req:
    :return:
    """

    if getattr(GeneralConfig, 'YCL_DRIVER_IS_SYNC', False):
        # Синхронный коннектор по требованию (был случай когда асинхронный тупо умирает из-за того что cl старый)

        with YandexSyncConnector(host, port, user, password, database) as ycl:
            cursor = ycl.cursor()
            cursor.execute(text_req)
            records = cursor.fetchall()
            if cursor.columns_with_types and records:
                column_names = [item[0] for item in cursor.columns_with_types]
                records = [dict(zip(column_names, item)) for item in records]

            cursor.close()
            return records
    else:
        # Асинхронный коннектор по умолчанию
        async with YandexConnector(host=host, port=port, user=user, password=password, database=database) as ycl:
            async with ycl.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(text_req)
                records = cursor.fetchall()
                return records


async def exec_req_from_str_jinja(conn, jinja_pattern, render_data, jinja_folder=None):
    """
    Обобщение выполнение запроса по строковому шаблону jinja
    """

    sql_req = jinja_render_str_to_str(
        str_pattern=jinja_pattern,
        render=render_data,
        pattern_folder=jinja_folder
    )

    res = await do_execute(
        **conn,
        text_req=sql_req
    )

    return res


async def exec_req_from_file_jinja(conn, jinja_pattern, render_data, jinja_folder=None):
    """
    Обобщение выполнение запроса по файловому шаблону jinja
    """
    sql_pattern = jinja_render_to_str(
        src=jinja_pattern,
        render=render_data,
        pattern_folder=jinja_folder,
        smart_replace=True
    )
    res = await do_execute(
        **conn,
        text_req=sql_pattern
    )

    return res


"""
Метаданные
"""


async def get_metadata(conn, table_filter=None):
    """
    Получить метаданные таблиц
    Надстройка над драйвером, чтобы голову не ломать. Сразу получаем самое вкусное

    table_filter = ['имя таблицы']
    """

    data_scheme = await get_tables_from_database(
        table_filter=table_filter,
        **conn
    )
    if data_scheme:
        return ycl_ycl_convertations.main_ycl_ycl_json_converter(data_scheme)

    return None


async def get_tables_from_database(host, port, user, password, database, table_filter=None):
    """
    Получает схему метаданных для конкретной базы данных
    :param table_filter: список конкретных таблиц, если не указан - то все public таблицы
    :param host:
    :param port:
    :param user:
    :param password:
    :param database:
    :return:
    """

    sql_pattern = jinja_render_to_str(
        src=DEFAULT_JINJA_PATTERN_META,
        render={
            'db_name': database,
            'table_filter': table_filter,
        }
    )
    res = await do_execute(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        text_req=sql_pattern
    )

    return res


"""
CREATE TABLE - Создание таблиц
"""


async def create_table(conn, table_data):
    """
    Создание таблицы. Шаблон create_table.jinja

    table_data = {
        'table_name': 'test_table',
        # 'db_name': 'default',
        # 'cluster': 'test_cluster',
        'columns': {
            'k1': {
                'type': 'Int32',
                'dma': '',
                'ttl': ''
            },
            'k2': {
                'type': 'String',
                'dma': '',
                'ttl': ''
            }
        },
        # 'index': {
        #     'ind1': {
        #         'expr': '(k2 + 1)',
        #         'type': 'minmax',
        #         'gran': 3
        #     }
        # },
        'engine': 'Kafka()',
        'order_by': ['k1'],
        # 'partition_by': ['k1'],
        # 'primary_keys': ['k1'],
        # 'sample_by': ['k1'],
        # 'ttl': 'test ttl expr',
        'settings': {
            'kafka_broker_list': "'localhost:9092'",
            'kafka_topic_list': "'topic'",
            'kafka_group_name': "'group1'",
            'kafka_format': "'JSONEachRow'",
            'kafka_num_consumers': 4
        }
    }
    """

    res = await exec_req_from_file_jinja(
        conn=conn,
        jinja_pattern=DEFAULT_JINJA_PATTERN_CREATE_TABLE,
        render_data=table_data
    )

    return res


"""
CREATE DICTIONARY - Создание Словарей
"""


async def create_dict(conn, dict_data):
    """
    Создание словаря. Шаблон create_dict.jinja
    dict_data=
    {
        "name_dict": "__cl_smpb_nsi_test_nsi_alt_new_dict",
        # "db_name": "default",
        # "cluster": "test cluster",
        "fields": {
            "INN777": "String",
            "OKTMO": "String"
        },
        "func": {
            "INN777": "toString",
            "OKTMO": "toString"
        },
        "primary_key": "INN777",
        "sources": {
            "POSTGRESQL": {
                "port": "'5432'",
                "host": "'cl_smpb_759ae582-d784-4a6d-b052-67a11ddbb317_postgres'",
                "user": "'__test_app_core'",
                "password": "'__test_app_core'",
                "db ": "'__test_app_core'",
                "table": "'__cl_smpb_nsi_test_nsi_alt_new_dict'"
            }
        },
        "lay_out": "complex_key_hashed()",
        "life_time": "MIN 3600 MAX 5400"
    }
    """

    res = await exec_req_from_file_jinja(
        conn=conn,
        jinja_pattern=DEFAULT_JINJA_PATTERN_CREATE_DICT,
        render_data=dict_data
    )

    return res


async def create_or_update_dict(conn, dict_data):
    """
    Выполняем запрос на создание словаря в кликхаусе.
    Сначала его сбрасываем - потом создаём.
    Операция сброса ошибкой не считается
    """

    try:
        await delete_dictionary(conn=conn, name=dict_data['name_dict'], db=dict_data.get('db_name'))
    except Exception:
        pass

    res = await exec_req_from_file_jinja(
        conn=conn,
        jinja_pattern=DEFAULT_JINJA_PATTERN_CREATE_DICT,
        render_data=dict_data
    )

    return res


def create_sql_dict(dict_data, jinja_folder=None):
    """
    Разделение создания словаря. Часть 1. SQL запрос
    """

    sql_pattern = jinja_render_to_str(
        src=DEFAULT_JINJA_PATTERN_CREATE_DICT,
        render=dict_data,
        pattern_folder=jinja_folder
    )

    return sql_pattern


async def create_from_sql_dict(conn, sql_req):
    """
    Разделение создания словаря. Часть 2. Выполнение запроса
    """

    res = await do_execute(
        **conn,
        text_req=sql_req
    )

    return res


"""
CREATE MATERIALIZED VIEW - Материализованные представления  
"""


async def create_materialized_view(conn, view_data):
    """
    Создание материализованного представления. Шаблон create_mview.jinja
    view_data = {
        'view_name': 'test_view',
        # 'db_name': 'default',
        # 'cluster': 'test_cluster',
        'table_dst': 'merge_three_showcase_client1',
        'table_src': 'kafka_showcase_client1',
        'columns':
            {
                'k1': 'toString(k1)',
                'k2': 'toInt64(k2)'
            }
    }
    """

    res = await exec_req_from_file_jinja(
        conn=conn,
        jinja_pattern=DEFAULT_JINJA_PATTERN_CREATE_MVIEW,
        render_data=view_data
    )

    return res


"""
DROP - Удаление
"""


async def delete_db(conn, name, cluster=None):
    """
    Удаление базы данных
    DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
    """

    jinja_str = 'DROP DATABASE IF EXISTS {{data_base}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %}'

    render_data = {
        'data_base': name,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def delete_table(conn, name, is_temp=False, db=None, cluster=None):
    """
    Удаление таблицы
    DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
    """

    jinja_str = 'DROP {% if tmp %}TEMPORARY{% endif %} TABLE IF EXISTS {% if db_name %}{{db_name}}.' \
                '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %}'

    render_data = {
        'table_name': name,
        'tmp': is_temp,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def delete_dictionary(conn, name, db=None):
    """
    Удаление словаря
    DROP DICTIONARY [IF EXISTS] [db.]name
    """

    jinja_str = 'DROP DICTIONARY IF EXISTS {% if db_name %}{{db_name}}.{% endif %}{{dict_name}}'

    render_data = {
        'dict_name': name,
        'db_name': db,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def delete_view(conn, name, db=None, cluster=None):
    """
    Удаление представления
    DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster]
    """

    jinja_str = 'DROP VIEW IF EXISTS {% if db_name %}{{db_name}}.' \
                '{% endif %}{{view_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %}'

    render_data = {
        'view_name': name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


"""
ATTACH/DETACH - присоединить/отсоединить таблицу/представление 
Для материализованных представлений отсоединяется/присоединяется таблица
"""


async def attach_table(conn, name, db=None, cluster=None):
    """
    Присоединение таблицы
    ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
    """

    jinja_str = 'ATTACH TABLE IF NOT EXISTS {% if db_name %}{{db_name}}.' \
                '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %}'

    render_data = {
        'table_name': name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def detach_table(conn, name, db=None, cluster=None):
    """
    Отсоединение таблицы. Перманентность не используем
    DETACH TABLE|VIEW [IF EXISTS] [db.]name [PERMANENTLY] [ON CLUSTER cluster]
    """

    jinja_str = 'DETACH TABLE IF EXISTS {% if db_name %}{{db_name}}.' \
                '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %}'

    render_data = {
        'table_name': name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


"""
ALERT. Колонки таблиц
"""


async def column_add(conn, table, name, type, expr=None, codec=None, after_name=None, db=None, cluster=None):
    """
    Добавление колонки
    ALTER TABLE [db].name [ON CLUSTER cluster] ADD COLUMN [IF NOT EXISTS]
    name [type] [default_expr] [codec] [AFTER name_after | FIRST]
    """

    jinja_str = \
        'ALTER TABLE {% if db_name %}{{db_name}}.' \
        '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %} ' \
        'ADD COLUMN IF NOT EXISTS {{column_name}} {{column_type}} ' \
        '{% if default_expr %}{{default_expr}}{% endif %} {% if codec %}{{codec}}{% endif %} ' \
        '{% if after_name %}AFTER {{after_name}}{% else %}FIRST{% endif %}'

    render_data = {
        'table_name': table,
        'column_name': name,
        'column_type': type,
        'default_expr': expr,
        'codec': codec,
        'after_name': after_name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def column_modify(conn, table, name, type, expr=None, ttl=None, after_name=None, db=None, cluster=None):
    """
    Изменение колонки
    ALTER TABLE [db].name [ON CLUSTER cluster]
    MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL] [AFTER name_after | FIRST]
    """

    jinja_str = \
        'ALTER TABLE {% if db_name %}{{db_name}}.' \
        '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %} ' \
        'MODIFY COLUMN IF EXISTS {{column_name}} {{column_type}} ' \
        '{% if default_expr %}{{default_expr}}{% endif %} {% if ttl %}{{ttl}}{% endif %} ' \
        '{% if after_name %}AFTER {{after_name}}{% else %}FIRST{% endif %}'

    render_data = {
        'table_name': table,
        'column_name': name,
        'column_type': type,
        'default_expr': expr,
        'ttl': ttl,
        'after_name': after_name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def column_delete(conn, table, name, db=None, cluster=None):
    """
    Удаление колонки
    ALTER TABLE [db].name [ON CLUSTER cluster]
    DROP COLUMN [IF EXISTS] name
    """

    jinja_str = \
        'ALTER TABLE {% if db_name %}{{db_name}}.' \
        '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %} ' \
        'DROP COLUMN IF EXISTS {{column_name}}'

    render_data = {
        'table_name': table,
        'column_name': name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def column_rename(conn, table, old_name, new_name, db=None, cluster=None):
    """
    Переименовывание колонки
    ALTER TABLE [db].name [ON CLUSTER cluster]
    RENAME COLUMN column_name TO new_column_name
    """

    jinja_str = \
        'ALTER TABLE {% if db_name %}{{db_name}}.' \
        '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %} ' \
        'RENAME COLUMN {{old_name}} TO {{new_name}}'

    render_data = {
        'table_name': table,
        'old_name': old_name,
        'new_name': new_name,
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


async def column_comment(conn, table, name, comment, db=None, cluster=None):
    """
    Комментарий к колонке
    ALTER TABLE [db].name [ON CLUSTER cluster]
    COMMENT COLUMN IF EXISTS name 'Text comment'
    """

    jinja_str = \
        'ALTER TABLE {% if db_name %}{{db_name}}.' \
        '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %} ' \
        'COMMENT COLUMN IF EXISTS {{column_name}} {{comment_text}}'

    render_data = {
        'table_name': table,
        'column_name': name,
        'comment_text': f"'{comment}'",
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


"""
Изменение настроек 
"""


async def modify_settings(conn, table, settings, value, db=None, cluster=None):
    """
    Изменение значения поля Settings. Интересно сработает ли для кафки. Написано MergeTree only!
    https://clickhouse.tech/docs/ru/operations/settings/merge-tree-settings/

    ALTER TABLE [db].name [ON CLUSTER cluster]
    MODIFY SETTING name = value
    """

    jinja_str = \
        'ALTER TABLE {% if db_name %}{{db_name}}.' \
        '{% endif %}{{table_name}} {% if cluster %}ON CLUSTER {{cluster}}{% endif %} ' \
        'MODIFY SETTING {{settings_name}} = {{settings_value}}'

    render_data = {
        'table_name': table,
        'settings_name': settings,
        'settings_value': modify_settings_preparation_value(value),
        'db_name': db,
        'cluster': cluster,
    }

    res = await exec_req_from_str_jinja(
        conn=conn,
        jinja_pattern=jinja_str,
        render_data=render_data
    )

    return res


def modify_settings_preparation_value(value):
    """
    Подготовка значений параметров настроек для вставки.
    Строковые значения должны быть обёрнуты в '' и только так
    """
    if isinstance(value, str):
        if value.startswith('"'):
            value = "'" + value[1:]

        if value.endswith('"'):
            value = value[:-1] + "'"

        if not value.startswith("'"):
            value = "'" + value

        if not value.endswith("'"):
            value = value + "'"
    return value

# # Пилотные тесты
# if __name__ == '__main__':
#     import asyncio
#
#
#     ioloop = asyncio.get_event_loop()
#     # ioloop.run_until_complete(get_tables_from_database("localhost", 9000, 'default', '', 'default'))
#     ioloop.run_until_complete(
#         do_execute("localhost", 9000, 'default', '', 'default', 'SELECT * FROM system.dictionaries'))
#     ioloop.close()
