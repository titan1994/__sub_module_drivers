"""
Парсинг метаданных кликхауса
"""


def main_ycl_ycl_json_converter(metadata_ycl, transform_add=None):
    """
    Конвертация метаданных кликхауса в кликхаус
    просто парсинг
    :return:
    """

    tables = []
    table_name = ''
    attributes = None

    if not isinstance(transform_add, dict):
        transform_add = {}

    conv_name = transform_add.get('conv_name', {})
    conv_dict = transform_add.get('conv_dict', {})

    for column_data in metadata_ycl:

        dict_type = column_data.get('dict_type')
        current_table_name = column_data['table_name']
        current_column_name = column_data['column_name']

        column = {
            'name': current_column_name,
            '_name': conv_name.get(current_table_name, {}).get(current_column_name, current_column_name),
            'type': column_data['column_type'],
            'comment': column_data['column_comment'],
        }

        # Хардкод отношений таблиц
        dict_rel = conv_dict.get(current_table_name, {}).get(current_column_name)
        if dict_rel:
            column['dictionary'] = dict_rel

        # Определение первичного ключа
        if dict_type:
            # Кто бы мог подумать но в словаре первичный ключ не храниться в системной таблице...
            create_table_query = column_data['create_table_query']
            posA = create_table_query.find('PRIMARY KEY')
            posB = create_table_query.find('SOURCE')

            if posA != -1 and posB != -1:
                # Внешние словари
                prim_keys = create_table_query[posA + len('PRIMARY KEY'):posB]
                if current_column_name in prim_keys:
                    column['is_pk'] = True
                else:
                    column['is_pk'] = False
            else:
                # Внутренние словари
                if current_column_name in column_data['primary_key']:
                    column['is_pk'] = True
                else:
                    column['is_pk'] = False
        else:
            # C нормальными таблицами всё хорошо

            if current_column_name in column_data['primary_key']:
                column['is_pk'] = True
            else:
                column['is_pk'] = False

        # Добавление информации про таблицу
        if not table_name == current_table_name:
            if attributes is not None:
                table = {
                    'name': table_name,
                    '_name': table_human_name,
                    'engine': engine,
                    'is_dict': is_dict,
                    'attributes': attributes,
                }
                tables.append(table)

            table_name = current_table_name
            table_human_name = conv_name.get(current_table_name, {}).get('_', current_table_name)
            engine = column_data.get('table_engine')
            is_dict = True if dict_type else False

            attributes = []

        attributes.append(column)

    # Последняя таблица
    table = {
        'name': table_name,
        '_name': table_human_name,
        'engine': engine,
        'is_dict': is_dict,
        'attributes': attributes,
    }

    tables.append(table)

    return tables
