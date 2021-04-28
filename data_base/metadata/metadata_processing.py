"""
Обработка метаданных по запросу
"""
from json import load as jsl

from GENERAL_CONFIG import GeneralConfig

DEFAULT_FILE_META_CONV = {
    'conv_name': 'convert_names.json',
    'conv_dict': 'convert_relation_dict.json'
}

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
    transform_add = {}

    # Выясняем надо ли нам использовать хардкод
    PATTERN_META_CONV_FOLDER = getattr(GeneralConfig, 'PATTERN_META_CONV_FOLDER', None)
    if PATTERN_META_CONV_FOLDER:
        for conv_name, conv_file in DEFAULT_FILE_META_CONV.items():
            try:
                with open(PATTERN_META_CONV_FOLDER / conv_file, 'r', encoding='utf-8') as file:
                    transform_add[conv_name] = jsl(fp=file)
            except Exception:
                continue

    conv_name = transform_add.get('conv_name', {})
    conv_rel_dict = transform_add.get('conv_dict', {})

    # Преобразование водных данных для получения метаданных
    for schema in json_in:
        try:
            ip = schema['ip']
            port = schema['port']
            data_bases = schema['data_bases']

            name_hosts = f'{ip}:{port}'
            type = schema.get('type_db', 'ycl')

            storage_names = conv_name.get(name_hosts, {})
            storage_rels = conv_rel_dict.get(name_hosts, {})

            out_data_bases = []
            out_schema = {
                'ip': ip,
                'port': port,
                'type': type,
                '_name': storage_names.get('_', name_hosts),
                'name_schema': schema.get('name', name_hosts),
                'data_bases': out_data_bases,
            }
            # Каждую базу данных обраатываем отдельно
            for db in data_bases:
                try:
                    login = db.get('user', 'default')
                    db['user'] = login

                    pswd = db.get('pswd', '')
                    db['pswd'] = ''

                    db_name = db.get('name', 'default')
                    db['name'] = 'default'

                    db_tables = db.get('tables', '')
                    db['tables'] = ''

                    data_base_names = storage_names.get(db_name, {})
                    data_base_rels = storage_rels.get(db_name, {})

                    out_data_base = {
                        'name': db_name,
                        '_name': data_base_names.get('_', db_name)
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
                        out_tables = psql_cl_convertations.main_psql_ycl_json_converter(
                            metadata_psql=data_scheme,
                            transform_add={
                                'conv_name': data_base_names,
                                'conv_dict': data_base_rels
                            }
                        )

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
                        out_tables = ycl_ycl_convertations.main_ycl_ycl_json_converter(
                            metadata_ycl=data_scheme,
                            transform_add={
                                'conv_name': data_base_names,
                                'conv_dict': data_base_rels
                            }
                        )

                    out_data_base['tables'] = out_tables
                    out_data_bases.append(out_data_base)

                except Exception as exp:
                    out_data_bases.append(
                        {
                            'invalid_data': db,
                            'error': str(exp)
                        }
                    )

            # Массив ответов
            array_out.append(out_schema)

        except Exception as exp:
            array_out.append(
                {
                    'invalid_data': schema,
                    'error': str(exp)
                }
            )

    return array_out
