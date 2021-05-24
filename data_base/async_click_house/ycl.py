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

# Паттерны универсальных селектов

DEFAULT_JINJA_PATTERN_SELECT_SKD_TWO_GROUPS = Path(__file__).parent / 'select_skd_two_groups.jinja'

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


async def do_execute(host, port, user, password, database, text_req, manual_sync=False):
    """
    Проброс execute
    :param host:
    :param port:
    :param user:
    :param password:
    :param database:
    :param text_req:
    :param manual_sync: - Ручное переключение на синхронный драйвер
    :return:
    """

    if getattr(GeneralConfig, 'YCL_DRIVER_IS_SYNC', False) or manual_sync:
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


async def exec_req_from_str_jinja(conn, jinja_pattern, render_data, jinja_folder=None, manual_sync=False):
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
        text_req=sql_req,
        manual_sync=manual_sync
    )

    return res


async def exec_req_from_file_jinja(conn, jinja_pattern, render_data, jinja_folder=None, manual_sync=False):
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
        text_req=sql_pattern,
        manual_sync=manual_sync
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
        return ycl_ycl_convertations.main_ycl_ycl_json_converter(
            metadata_ycl=data_scheme,
            transform_add=transform_add
        )

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


async def system_reload_all_dictionaries(conn):
    """
    Перезагрузка всех словарей
    SYSTEM RELOAD DICTIONARIES
    """

    res = await do_execute(
        **conn,
        text_req='SYSTEM RELOAD DICTIONARIES'
    )

    return res


async def system_reload_all_embedded_dictionaries(conn):
    """
    Перезагрузка всех встроенных словарей
    SYSTEM RELOAD EMBEDDED DICTIONARIES
    """

    res = await do_execute(
        **conn,
        text_req='SYSTEM RELOAD EMBEDDED DICTIONARIES'
    )

    return res


async def system_reload_dictionaries(conn, names):
    """
    Перезагрузка словарей по именам
    SYSTEM RELOAD DICTIONARY Dictionary_name
    """

    jinja_str = 'SYSTEM RELOAD DICTIONARY {{dict_name}}'

    if isinstance(names, str):
        reload_list = [names]

    elif isinstance(names, list):
        reload_list = names
    else:
        reload_list = list[names]

    report = {}
    for name in reload_list:
        render_data = {
            'dict_name': name,
        }

        res = await exec_req_from_str_jinja(
            conn=conn,
            jinja_pattern=jinja_str,
            render_data=render_data
        )

        report[name] = res

    return report


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


"""
Хитрые/универсальные селекты, заслуживающие быть вписанными в драйвер
"""


class SKDSettingsError(Exception):
    pass


async def select_skd_two_groups(conn, data_select):
    """
    Уникальный селект. Создаёт выборку двойной вложенности без роллапа.
    Ну как бы делаем часть роллапа без него, но с увеличенным количеством измерений
    По сути - одна группировка, вложенная в другую - аналог СКД, но только на 2 уровня.
    Есть потенциал расшириться на несколько уровней

    Шаблон select_skd_two_groups.jinja

    Пример:
    skd_settings = {
        'table_name': '__cl_smpb_showcase_data_farmerpassport_meansofpassport',
        # 'db_name': 'defalut',
        'dimensions': [
            'organization',
            'subject',
            'period',
            'type',
            'economic_indicator',
            'source_system',
            'source_form',
        ],
        'two_levels': ['economic_indicator', 'source_form'],
        'mesures': [
            {
                'name': 'value',
                'func': 'SUM'
            }
        ],

        'filters': [
            {
                'name': 'type',
                'value': 'Факт',
            },
            {
                'union_start': True,
                'name': 'organization',
                'value': '6829004230',
            },
            {
                'condition': 'OR',
                'name': 'subject',
                'compare_func': 'IN',
                'value': ['65', '67'],
                'union_and': True,
            },
        ]

    }
    """

    jinja_settings = skd_group_two_processing_settings_to_jinja(data_select)

    res = await exec_req_from_file_jinja(
        conn=conn,
        jinja_pattern=DEFAULT_JINJA_PATTERN_SELECT_SKD_TWO_GROUPS,
        render_data=jinja_settings
    )

    return res


def skd_group_two_processing_settings_to_jinja(skd_settings):
    """
    Обработка настроек двойной группировки
    """

    DEFAULT_COLUMN_NAME_ORDER = '__SYSTEM_ORDER_MAPPING__'

    # Проверка ключевых параметров

    table_name = skd_settings.get('table_name', None)
    if not table_name:
        raise SKDSettingsError('name table not found!')

    dimensions = skd_settings.get('dimensions', None)
    if not dimensions:
        raise SKDSettingsError('dimensions not found!')

    mesures = skd_settings.get('mesures', None)
    if not mesures:
        raise SKDSettingsError('measures not found!')

    level_list = skd_settings.get('two_levels', None)
    if not level_list:
        raise SKDSettingsError('two levels not found!')

    # Оценка уровней результата
    ln_dim = len(dimensions)

    level_names = {}
    count_level = 0
    for name_lv in level_list:
        if name_lv in dimensions:
            level_names[name_lv] = dimensions.index(name_lv)
            count_level = count_level + 1
            if count_level >= 2:
                break

    if not level_names:
        raise SKDSettingsError('invalid levels!')

    # Создание настроек для джинджы по уровням
    jinja_settings = {
        'table_name': table_name,
        'tables': [],
        'order_by': [],
        'db_name': skd_settings.get('db_name', None),
    }

    filters = skd_settings.get('filters', None)
    if filters:
        jinja_settings['filters'] = skd_filter_processing_settings(filters)

    order_count = 0
    for name_dim, ind in level_names.items():

        if order_count == 0:
            order_str = f'{order_count} as {DEFAULT_COLUMN_NAME_ORDER}'
        else:
            order_str = f'{order_count}'

        fields = dimensions[:ind + 1]
        if ind < (ln_dim - 1):
            for index_dim in range(ind, ln_dim - 1):
                fields.append(f'NULL as {dimensions[index_dim+1]}')

        fields.append(order_str)

        for measure in mesures:
            if measure.get('func', None):
                field_str = f"{measure['func']}({measure['name']})"
            else:
                field_str = f"{measure['name']}"

            fields.append(field_str)

        table_settings = {
            'fields': fields,
            'group_fields': dimensions[:ind + 1],
        }

        jinja_settings['tables'].append(table_settings)
        order_count = order_count + 1

    jinja_settings['order_by'] = jinja_settings['tables'][0]['group_fields'].copy()
    jinja_settings['order_by'].append(DEFAULT_COLUMN_NAME_ORDER)

    return jinja_settings


def skd_filter_processing_settings(filters):
    """
    Предварительная обработка филтров и их значений
    """

    new_filters = []

    for filter in filters:
        new_filter = {
            'condition': filter.get('condition', None),
            'union_start': filter.get('union_start', None),
            'name': filter['name'],
            'compare_func': filter.get('compare_func', None),
            'value': skd_type_processing_filter(filter['value']),
            'union_and': filter.get('union_and', None),
        }

        new_filters.append(new_filter)

    return new_filters


def skd_type_processing_filter(value):
    """
    Обработка значений и преобразование под селект
    """

    result = f'{value}'

    if isinstance(value, str):
        result = f"'{value}'"

    elif isinstance(value, list):

        jinja_str = """
        (
            {% set ns = namespace(first=False)  %}
            {% for field in fields %}{% if ns.first %},
            {% else %}{% set ns.first = True %}{% endif%}{{field}}{% endfor %}
        )

        """

        new_values = []
        for val in value:
            new_values.append(skd_type_processing_filter(val))

        render_data = {
            'fields': new_values,
        }

        result = jinja_render_str_to_str(
            str_pattern=jinja_str,
            render=render_data
        )

    return result


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
