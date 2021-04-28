"""
Основная конфигурация проекта.
Сначала мы решаем какое ядро используем - потом наследуем его в GeneralConfig
"""

from pathlib import Path


class GeneralConfig:
    """
    Общая конфа - она импортируется по проекту
    """
    ITS_DOCKER = None
    KAFKA_URL = None

    # Синхронный драйвер - удалить/заменить на False - если нужен асинхронный
    # YCL_DRIVER_IS_SYNC = True

    #  Задать папку дополнительной конвертации метаданных
    PATTERN_META_CONV_FOLDER = Path(__file__).parent / '__driver_pattern_db_meta_conv'
