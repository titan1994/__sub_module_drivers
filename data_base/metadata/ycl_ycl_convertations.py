"""
Парсинг метаданных кликхауса
"""


def main_ycl_ycl_json_converter(metadata_ycl):
    """
    Конвертация метаданных кликхауса в кликхаус
    просто парсинг
    :return:
    """

    tables = []
    table_name = ''
    attributes = None

    for column_data in metadata_ycl:

        column = {
            'name': column_data['column_name'],
            'type': column_data['column_type'],
            'comment': column_data['column_comment'],
        }

        dict_type = column_data.get('dict_type')
        if dict_type:
            # Кто бы мог подумать но в словаре первичный ключ не храниться в системной таблице...
            create_table_query = column_data['create_table_query']
            posA = create_table_query.find('PRIMARY KEY')
            posB = create_table_query.find('SOURCE')

            prim_keys = create_table_query[posA + len('PRIMARY KEY'):posB]
            if column_data['column_name'] in prim_keys:
                column['is_pk'] = True
            else:
                column['is_pk'] = False
        else:
            # C нормальными таблицами всё хорошо

            if column_data['column_name'] in column_data['primary_key']:
                column['is_pk'] = True
            else:
                column['is_pk'] = False

        if not table_name == column_data['table_name']:
            if attributes is not None:
                table = {
                    'name': table_name,
                    'engine': engine,
                    'is_dict': is_dict,
                    'attributes': attributes
                }
                tables.append(table)

            table_name = column_data['table_name']
            engine = column_data.get('table_engine')
            is_dict = True if dict_type else False
            attributes = []

        attributes.append(column)

    # Последняя таблица
    table = {
        'name': table_name,
        'attributes': attributes
    }

    tables.append(table)

    return tables
