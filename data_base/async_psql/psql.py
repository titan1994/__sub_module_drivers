"""
Асинхронный драйвер постгреса
"""

import asyncio
import asyncpg
from pathlib import Path
from MODS.scripts.python.jinja import jinja_render_to_str, jinja_render_str_to_str
from ..metadata import psql_cl_convertations
from ..async_click_house.ycl import skd_filter_processing_settings

DEFAULT_JINJA_PATTERN_META = Path(__file__).parent / 'get_meta.jinja'
DEFAULT_JINJA_PATTERN_LEFT_JOIN_SIMPLE = Path(__file__).parent / 'select_left_join_simple.jinja'

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

"""
Выборки
"""
async def get_left_join_simple(conn, render_data):
    """
    Сделать простую выборку с left join

    render_data: dict = {
            left_fields: list (список полей левой таблицы)
            right_fields: list (список полей правой таблицы)
            table_left_short: str (короткое название левой таблицы, не обязательно)
            table_right_short: str (короткое название правой таблицы)
            table_right_short: str (короткое название правой таблицы)
            filters: list (filters)
            table_right: str (полное название правой таблицы)
            table_left: str (полное название левой таблицы)
            field_left: str (название поля правой таблицы, которое приравняется в ON)
            field_right: str (название поля левой таблицы, которое приравняется в ON)
        }
    """
    jinja_pattern = DEFAULT_JINJA_PATTERN_LEFT_JOIN_SIMPLE
    render_data['fields'] = []
    render_data['table_left_short'] = render_data.get('table_left_short', 'left_table')
    render_data['table_right_short'] = render_data.get('table_right_short', 'right_table')
    for left_field in render_data.get('left_fields', []):
        render_data['fields'].append(render_data.get('table_left_short')+'.'+left_field)
    for right_field in render_data.get('right_fields', []):
        render_data['fields'].append(render_data.get('table_right_short')+'.'+right_field)
    if render_data.get('filters', None) is not None:
        render_data['filters'] = skd_filter_processing_settings(render_data['filters'])

    result = await exec_req_from_file_jinja(conn, jinja_pattern, render_data)

    return result