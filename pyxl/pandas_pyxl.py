"""
API Pandas + openpyxl
"""

import openpyxl
import pandas as pd
from os import remove


class PandasPyXlRW:
    """
    Инициализация пандаса и openpyxl вместе
    """

    def __init__(self, file_path, child_path=None, delete_parent=False):
        """

        :param file_path: Путь к файлу экселя или файловый поток
        :param child_path: Путь альтернативного сохранения (потомок)
        :param delete_parent: Удалить родителя (если указан потомок)
        """

        self.file_path = file_path
        self.child_path = child_path
        self.delete_parent = delete_parent
        self.pyxl_wb = None
        self.pd_writer = None

    def __enter__(self):
        """
        4-ре строчки добытые  вбою

        :return:
        """
        wb = openpyxl.load_workbook(self.file_path)
        writer = pd.ExcelWriter(self.file_path, engine='openpyxl', mode='a')
        writer.book = wb
        writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)

        self.pyxl_wb = wb
        self.pd_writer = writer

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Сохранить согласно настройкам инициализации

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """

        try:
            self.save_data()
        except Exception:
            pass

        try:
            self.pyxl_wb.close()
            self.pd_writer.close()
        except Exception:
            pass

    def save_data(self):
        """
        Для случаев обработки стрима от временного файла
        :return:
        """
        if self.child_path:
            self.pyxl_wb.save(self.child_path)
            self.remove_parent()
        else:
            self.pyxl_wb.save(self.file_path)

    def remove_parent(self):
        """
        Удаление родителя

        :return:
        """
        if self.delete_parent:
            try:
                remove(self.file_path)
            except Exception:
                pass

    @classmethod
    def create_tsv_from_xlsx(cls):
        pass