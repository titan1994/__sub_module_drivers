# Адрес всех доступных брокеров кафки глазами приложения и глазами кликхауса
from GENERAL_CONFIG import GeneralConfig
from os import getenv

if GeneralConfig.ITS_DOCKER:
    GeneralConfig.KAFKA_URL = getenv('KAFKA_URL_DOCKER')
    GeneralConfig.YCL_KAFKA_URL = GeneralConfig.KAFKA_URL
else:
    GeneralConfig.KAFKA_URL = getenv('KAFKA_URL_NO_DOCKER')


