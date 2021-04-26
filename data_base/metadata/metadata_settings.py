"""
Типовые блоки коннекторов к базам
"""
from enum import Enum


class DataBaseTypes(Enum):
    psql = 'psql'
    mysql = 'mysql'
    ycl = 'ycl'
