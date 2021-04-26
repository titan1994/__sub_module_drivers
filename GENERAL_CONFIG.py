"""
Основная конфигурация проекта.
Сначала мы решаем какое ядро используем - потом наследуем его в GeneralConfig
"""

from MODS.scripts.python.easy_scripts import PROJECT_GENERAL_FOLDER as general_path
from multiprocessing import cpu_count


class GeneralConfig():
    """
    Общая конфа - она импортируется по проекту
    """

    KAFKA_URL = None

