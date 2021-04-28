"""
Обработка метаданных по запросу
"""
from . import metadata_settings

# psql - ycl
from ..async_psql import psql
from . import psql_cl_convertations

# ycl - ycl
from ..async_click_house import ycl
from . import ycl_ycl_convertations


async def process_metadata(json_in):
    """
    Обработка метаданных. Главный обработчик
    :param json_in:
    :return:
    """
    array_out = []

    for schema in json_in:

        ip = schema['ip']
        port = schema['port']
        type = schema['type_db']
        data_bases = schema['data_bases']
        name_schema = schema['name']

        out_data_bases = []

        out_schema = {
            'ip': ip,
            'port': port,
            'type': type,
            'name_schema': name_schema,
            'data_bases': out_data_bases,
        }

        for db in data_bases:

            login = db['user']
            pswd = db['pswd']
            db_name = db['name']
            db_tables = db['tables']

            out_data_base = {
                'name': db_name,
            }

            if metadata_settings.DataBaseTypes(type) == metadata_settings.DataBaseTypes.psql:
                # Постгрес в кликхаус

                data_scheme = await psql.get_tables_from_database(
                    host=ip,
                    port=port,
                    user=login,
                    password=pswd,
                    database=db_name,
                    table_filter=db_tables
                )
                out_tables = psql_cl_convertations.main_psql_ycl_json_converter(data_scheme)

            if metadata_settings.DataBaseTypes(type) == metadata_settings.DataBaseTypes.ycl:
                # Кликхаус в кликхаус

                data_scheme = await ycl.get_tables_from_database(
                    host=ip,
                    port=port,
                    user=login,
                    password=pswd,
                    database=db_name,
                    table_filter=db_tables
                )
                out_tables = ycl_ycl_convertations.main_ycl_ycl_json_converter(data_scheme)

            out_data_base['tables'] = out_tables
            out_data_bases.append(out_data_base)

        array_out.append(out_schema)

    return array_out
