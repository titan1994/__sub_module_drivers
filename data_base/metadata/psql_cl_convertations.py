"""
Конвертор метаданных постгркса в кликхаус
"""

from enum import Enum


# Возможные движки кликхауса
class EngineBase(Enum):
    MergeTree = 'MergeTree()'
    ReplacingMergeTree = 'ReplacingMergeTree()'
    SummingMergeTree = 'SummingMergeTree()'
    AggregatingMergeTree = 'AggregatingMergeTree()'
    CollapsingMergeTree = 'CollapsingMergeTree()'
    VersionedCollapsingMergeTree = 'VersionedCollapsingMergeTree()'
    GraphiteMergeTree = 'GraphiteMergeTree()'


DEFAULT_ENGINE = EngineBase.MergeTree

# Одна большая партянка на все случаи жиизни. типы PSQL в типы кликхауса.
# Оказалось, кликхаусу не нужны ограничители длин строк и прочее количество разрядов и так далее
TYPE_TRANSFORM_ACCORDING_PSQL_TO_CL = {

    # Числа (тип decimal учтен кодом-парсером)
    'smallint': 'Int16',
    'integer': 'Int32',
    'bigint': 'Int64',
    'real': 'Float32',
    'double precision': 'Float64',
    'smallserial': 'UInt16',
    'serial': 'UInt32',
    'bigserial': 'UInt64',
    'money': 'Int64',

    # Строки
    'character': 'VARCHAR',
    'character varying': 'VARCHAR',
    'char': 'VARCHAR',
    'varchar': 'VARCHAR',
    'text': 'VARCHAR',

    # Булева нет - хранят в 1м байте
    # https://clickhouse.tech/docs/ru/sql-reference/data-types/boolean/
    'boolean': 'UInt8',

    # Вызвал противоречия - запихал в строку
    # https://clickhouse.tech/docs/ru/sql-reference/data-types/multiword-types/
    'bytea': 'VARCHAR',

    # Дата-время
    'timestamp': 'DateTime64',
    'date': 'DateTime64',
    'time': 'DateTime64',
    # 'interval': '', - целенаправлено не преобразуется - куда его не понятно

    # Перечисление
    # 'ENUM': 'Enum16(значения перечислений)', для этого необходимо сбегать в постгрес и уточнить, делать не будем

    # Геометрические фигуры
    # Не знаю зачем, но я подумал что они залезут спокойно в массив интов
    #
    # https://www.postgresql.org/docs/9.5/datatype-geometric.html

    'point': 'Array(Int32)',
    'line': 'Array(Int32)',
    'lseg': 'Array(Int32)',
    'box': 'Array(Int32)',
    'path': 'Array(Int32)',
    'polygon': 'Array(Int32)',
    'circle': 'Array(Int32)',

    # Интернет
    # Не особо бьётся с концепцией хранения клик-хауса. Но как-то так вобщем
    # https://www.postgresql.org/docs/9.5/datatype-net-types.html
    # https://clickhouse.tech/docs/ru/sql-reference/data-types/domains/ipv6/

    'cidr': 'IPv4',
    'inet': 'IPv6',
    'macaddr': 'UInt64',  # - Мак адрес может сюда поместиться - но будет ли, станет понятно на практике

    # Битовые строки
    # Тоже не ясно куда их. Затолкал в строки
    # https://www.postgresql.org/docs/9.5/datatype-bit.html
    'bit': 'VARCHAR',
    'bit varying': 'VARCHAR',

    # Строки поиска
    # Если кто найдёт куда - буду рад, если поправите.
    # Я плюнул на это дело и просто всё невнятное буду писать в строку
    # https://www.postgresql.org/docs/9.5/datatype-textsearch.html
    'tsvector': 'VARCHAR',
    'tsquery': 'VARCHAR',

    # uuid
    'uuid': 'UUID',

    # pg_lsn
    # https://www.postgresql.org/docs/9.5/datatype-pg-lsn.html
    'pg_lsn': 'UInt64',

    # json, xml snapshot
    'json': 'VARCHAR',
    'jsonb': 'VARCHAR',

    'txid_snapshot': 'VARCHAR',
    'xml': 'VARCHAR',

    # Для синонимов и массивов ()
    'int': 'Int32',
    'int2': 'Int16',
    'int4': 'Int32',
    'int8': 'Int64',

    'serial2': 'UInt16',
    'serial4': 'UInt32',
    'serial8': 'UInt64',

    'varbit ': 'VARCHAR',
    'bool': 'UInt8',
    'float4': 'Float32',
    'float8': 'Float64',

    'timetz': 'DateTime64',
    'timestamptz': 'DateTime64',
}


def get_data_type_column(column_data):
    """
    Получение типа колонки
    :param column_data:
    :return:
    """

    data_type = column_data['data_type']
    data_type_scan = TYPE_TRANSFORM_ACCORDING_PSQL_TO_CL.get(data_type.lower())

    if data_type_scan is None:

        if data_type in ['decimal', 'numeric']:
            precision = column_data['numeric_precision']
            scale = column_data['numeric_scale']

            data_type_scan = f'Decimal64({precision},{scale})'

        else:
            # В любом другом случае пробуем распарсить синоним (в том числе если тип ARRAY)
            data_type_found = column_data['udt_name'].lower()
            data_type_scan = TYPE_TRANSFORM_ACCORDING_PSQL_TO_CL.get(data_type_found.replace('_', ''))

    return data_type_scan


def main_psql_ycl_json_converter(metadata_psql):
    """
    Конвертация метаданных постгреса в кликхаус, предполагаемый селект:

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

    :return:
    """

    tables = []
    table_name = ''
    attributes = None

    for column_data in metadata_psql:

        data_type_scan = get_data_type_column(column_data)

        if data_type_scan is None:
            data_type_scan = 'TYPE_TRANSFORM_ERROR'

        column = {
            "name": column_data['column_name'],
            "type": data_type_scan
        }

        if not table_name == column_data['table_name']:
            if attributes is not None:
                table = {
                    'name': table_name,
                    'attributes': attributes
                }

                tables.append(table)

            table_name = column_data['table_name']
            attributes = []

        attributes.append(column)

    # Последняя таблица
    table = {
        'name': table_name,
        'attributes': attributes
    }

    tables.append(table)

    return tables
