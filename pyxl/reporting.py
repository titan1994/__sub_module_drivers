"""
Пример формирования отчёта по шаблону экслея из базы
Он привязан к GINO ORM поэтому естественно не работает из коробки
Он просто выдернут из проекта. Но если нужно очень - то можно просто первую функцию модифицировать

"""

import pandas as pd
import numpy as np
from tempfile import TemporaryDirectory
from pathlib import Path
from uuid import uuid4


# from app_core.core_models.general import Reporting, PatternVersion
# from app_core.core_models.general import MixinFilePattern

class Reporting:
    pass


class PatternVersion:
    pass


class MixinFilePattern:
    pass


from MODS.DRIVERS.pyxl.pandas_pyxl import PandasPyXlRW

"""
Генерация отчетов 
"""


async def generate_reporting(json_array):
    """
    Генерация отчётов

    :param json_array:
    :return:
    """

    js_out = []
    for pattern in json_array:
        # шаблон для формирования

        dict_out = {
            'index': json_array.index(pattern),
            'status': False,
            'msg': 'UNKNOWN ERROR',
            'data': []
        }

        try:
            # Подготовка первичных параметров. Определение шаблона
            pattern_id = pattern.get('pattern_id')
            pattern_version_id = pattern.get('pattern_version_id')
            pattern_file = pattern.get('pattern_file')

            if pattern_version_id:
                pw = await PatternVersion.get(pattern_version_id)
                pattern_file = pw.file
                pattern_id = pw.pattern_id

            elif pattern_id:
                pw = await PatternVersion.query.where(
                    PatternVersion.pattern_id == pattern_id,
                    PatternVersion.is_actual
                ).gino.first()

                pattern_file = pw.file
                pattern_version_id = pw.id

            elif pattern_file:
                pattern_file = MixinFilePattern.file_create(pattern_file)

            if pattern_file is None:
                raise Exception('pattern not found')

            user_id = pattern.get('author')
            if user_id is None:
                raise Exception('author/user is not set')

            # Вставка шаблона в файл
            with TemporaryDirectory() as tmp_dir:
                file_path_pattern = Path(tmp_dir) / f'{uuid4()}.xlsx'

                # Создание двоичных данных шаблона
                with open(file_path_pattern, 'wb') as fobj:
                    fobj.write(pattern_file)

                # Вставка данных в шаблон
                with PandasPyXlRW(file_path_pattern) as pd_xl:
                    for data_sheet in pattern['data']:
                        # лист, на который необходимо вставить данные
                        try:
                            sheet = pd_xl.pyxl_wb.worksheets[data_sheet['sheets']['name_id']]
                            start_row = data_sheet['sheets']['start_position']['row']

                        except (KeyError, AttributeError):
                            continue
                        # Поиск колонок - они либо явно заданы, либо это шаблон из 1С (поиск по значению в ячейке)
                        columns = processing_xl_found_column(data_sheet, sheet, start_row)

                        # Построить колоночный ряд для вставки
                        start_column, column_areas = processing_xl_generate_column(data_sheet, columns)

                        # Заполнить именованные области, если они есть (глобально, тоесть не на определенном листе)
                        processing_xl_set_named_areas(data_sheet, pd_xl)

                        # Заполнить статические параметры
                        try:
                            new_start = processing_xl_insert_static_data(
                                pd_xl,
                                data_sheet,
                                sheet,
                                columns,
                                column_areas,
                                start_column
                            )

                            start_row = new_start
                        except Exception:
                            pass

                        # Заполнить динамические параметры
                        try:
                            processing_xl_insert_dynamic_data(
                                pd_xl,
                                data_sheet,
                                sheet,
                                columns,
                                column_areas,
                                start_row,
                                start_column
                            )
                        except Exception:
                            pass

                # Получение двоичных данных отчета
                with open(file_path_pattern, 'rb') as fobj:
                    file_data = fobj.read()

                # Кодировка в base64 - и помещение в базу данных
                if pattern_id:
                    dict_create = {
                        'pattern_id': pattern_id,
                        'pattern_version_id': pattern_version_id,
                        'author': user_id,
                        'file': file_data
                    }

                    report = await Reporting.create(**dict_create)
                    data_out = report.to_dict()
                else:
                    data_out = {'file': MixinFilePattern.file_select(file_data), 'author': user_id}

                dict_out['msg'] = "OK"
                dict_out['status'] = True
                dict_out['data'] = data_out

        except Exception as exp:
            dict_out['msg'] = f"{exp}"
            pass

        js_out.append(dict_out)

    return js_out


def processing_xl_found_column(data_sheet, sheet, start_row):
    """
    Пропарсить колонки, найти их в экселе. Создать словарь

    :param data_sheet:
    :param sheet:
    :param start_row:
    :return:
    """

    columns = {}

    found_columns = data_sheet.get('columns_ids')
    if found_columns is None:
        # Интеграция с шаблонами 1С (поиск имени ячейки по шаблону)

        found_columns = data_sheet.get('cell_columns')
        if found_columns is not None:

            iterable_cells = sheet.iter_cols(
                min_col=1,
                max_col=sheet.max_column,
                min_row=start_row,
                max_row=start_row
            )

            for cells in iterable_cells:
                cell = cells[0]
                if cell.value in found_columns:
                    columns[cell.coordinate] = cell
    else:
        #  Обычный порядок вставки

        for cells in sheet.iter_cols(1, sheet.max_column):
            cell = cells[0]
            if cell.coordinate in found_columns or cell.col_idx in found_columns:
                columns[cell.coordinate] = cell

    return columns


def processing_xl_generate_column(data_sheet, columns):
    """
    Сгенерировать колоночный ряд и первую колонку на вставку
    :param data_sheet:
    :param columns:
    :return:
    """

    column_areas = []
    column_area = {}
    last_coordinate = list(columns.values())[0].col_idx
    first_column = None

    for _, cell in columns.items():

        if (cell.col_idx - last_coordinate) > 1:
            if len(column_area) > 0:
                column_areas.append(column_area)
                column_area = {}

        column_area[cell.coordinate] = cell.col_idx
        if first_column is None:
            first_column = cell.col_idx

        last_coordinate = cell.col_idx

    if len(column_area) > 0:
        column_areas.append(column_area)

    if first_column is None:
        # Нет колонок для вставки - значит это начальная позиция
        try:
            start_column = data_sheet['sheets']['start_position']['column']
        except (KeyError, AttributeError):
            start_column = 1
    else:
        start_column = first_column

    return start_column, column_areas


def processing_xl_set_named_areas(data_sheet, pd_xl):
    """
    Заполнение именованых областей

    :param data_sheet:
    :param pd_xl:
    :return:
    """
    named_areas = data_sheet.get('named_areas')
    if named_areas:
        for df_name in pd_xl.pyxl_wb.defined_names.definedName:
            value_nr = named_areas.get(df_name.name)
            if value_nr:
                df_name.attr_text = f'{value_nr}'


def processing_xl_insert_static_data(pd_xl, data_sheet, sheet, columns, column_areas, start_column):
    """
    Заполнение статических данных

    :param pd_xl:
    :param data_sheet:
    :param sheet:
    :param columns:
    :param column_areas:
    :param start_column:
    :return:
    """

    data = data_sheet.get('data_static')
    if data is None:
        return

    # Сортируем строки по возрастанию
    data.sort(key=lambda x: x['row'])

    # Вставка матриц. Строки могут идти не одна за одной а с прерыванием 1,2, 5,6, 9
    matrix_array = []

    current_row = data[0]['row']
    first_row = current_row

    for dict_data in data:

        this_row = dict_data['row']

        if (this_row - current_row) > 1:
            if len(matrix_array) > 0:
                df = pd.DataFrame(data=np.array(matrix_array), columns=columns.keys())
                processing_xl_insert_data_frame(df, pd_xl, sheet, column_areas, first_row, start_column)

                first_row = this_row
                matrix_array = []

        matrix_array.append(dict_data['data'])
        current_row = this_row

    if len(matrix_array) > 0:
        df = pd.DataFrame(data=np.array(matrix_array), columns=columns.keys())
        processing_xl_insert_data_frame(df, pd_xl, sheet, column_areas, first_row, start_column)

    # Сместить позицию для динамической вставки. Возврат новой стартовой позиции
    start_row = data[-1]['row'] + 1
    return start_row


def processing_xl_insert_dynamic_data(pd_xl, data_sheet, sheet, columns, column_areas, start_row, start_column):
    """
    Заполнение динамических данных

    :param pd_xl:
    :param data_sheet:
    :param sheet:
    :param columns:
    :param column_areas:
    :param start_row:
    :param start_column:
    :return:
    """

    data = data_sheet.get('data_dynamic')
    if data is None:
        return

    # Необходимо преобразовать в массив нампай перед вставкой np.array(data)
    df = pd.DataFrame(data=np.array(data), columns=columns.keys())

    processing_xl_insert_data_frame(df, pd_xl, sheet, column_areas, start_row, start_column)


def processing_xl_insert_data_frame(df, pd_xl, sheet, column_areas, start_row, start_column):
    """
    Вставка фрейма данных в эксель в зависимости от пустот меду колонками

    :param df:
    :param pd_xl:
    :param sheet:
    :param column_areas:
    :param start_row:
    :param start_column:
    :return:
    """

    if len(column_areas) > 0:
        # Между колонками есть пустоты. Вставка областями

        for column_area in column_areas:
            index_column_list = list(column_area.values())

            df.to_excel(
                pd_xl.pd_writer,
                sheet_name=sheet.title,
                startrow=start_row - 1,
                startcol=index_column_list[0] - 1,
                columns=column_area.keys(),
                header=False,
                index=False
            )
    else:
        # Прямая вставка без учёта пустот - их нет
        df.to_excel(
            pd_xl.pd_writer,
            sheet_name=sheet.title,
            startrow=start_row - 1,
            startcol=start_column - 1,
            header=False,
            index=False
        )
