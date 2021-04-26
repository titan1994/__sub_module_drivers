"""
Асинхронный драйвер постгреса
"""

import asyncio
import asyncpg


class PostgresConnector:
    """
    Драйвер коннектора
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

    sql_pattern = """
    SELECT * FROM
    information_schema.tables as tables
    LEFT
    JOIN
    INFORMATION_SCHEMA.COLUMNS as columns
    ON
    tables.table_name = columns.table_name
    WHERE
    tables.table_schema = 'public'
    <table_filter>
    """

    if table_filter:
        sql_filter_table = ""
        for table_name in table_filter:
            sql_filter_table = sql_filter_table + table_name + ','

        sql_filter_table = sql_filter_table[:-1]

        sql_pattern = sql_pattern.replace('<table_filter>', f'and tables.table_name IN ({sql_filter_table})')
    else:
        sql_pattern = sql_pattern.replace('<table_filter>', '')

    async with PostgresConnector(host, port, user, password, database) as psql:

        records = await psql.fetch(sql_pattern)
        return records

        # Можно перебирать запрос асинхронно и пачками - когда не важен порядок следования элементов
        # async with psql.transaction():
        #     async for rec in psql.cursor(sql_pattern):
        #         print(rec['column_name'])


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(get_tables_from_database("localhost", 5432, 'admin', 'admin', 'nifi_db'))
    ioloop.close()
