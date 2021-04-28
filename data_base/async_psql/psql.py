"""
Асинхронный драйвер постгреса
"""

import asyncio
import asyncpg
from pathlib import Path
from MODS.scripts.python.jinja import jinja_render_to_str, jinja_render_str_to_str
from ..metadata import psql_cl_convertations

DEFAULT_JINJA_PATTERN_META = Path(__file__).parent / 'get_meta.jinja'

"""
Обобщённый функционал
"""


class PostgresConnector:
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
        self.conn = await asyncpg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.conn.close()


async def do_execute(host, port, user, password, database, text_req):
    """
    Выполнение запроса к базе
    """
    async with PostgresConnector(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
    ) as psql:
        records = await psql.fetch(text_req)
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


async def get_metadata(conn, table_filter=None, transform_add=None):
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
        return psql_cl_convertations.main_psql_ycl_json_converter(
            metadata_psql=data_scheme,
            transform_add=transform_add
        )

    return None


async def get_tables_from_database(host, port, user, password, database, table_filter=None):
    """
    Получет схему метаданных для конкретной базы данных

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
