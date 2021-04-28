# Адрес всех доступных брокеров кафки глазами приложения и глазами кликхауса
from GENERAL_CONFIG import GeneralConfig
from os import getenv
from dotenv import load_dotenv

from MODS.scripts.python.easy_scripts import PROJECT_GENERAL_FOLDER

load_dotenv(PROJECT_GENERAL_FOLDER / '.env')
load_dotenv(PROJECT_GENERAL_FOLDER / '.env.production')

GeneralConfig.ITS_DOCKER = getenv('ITS_DOCKER')

if GeneralConfig.ITS_DOCKER:
    GeneralConfig.KAFKA_URL = getenv('KAFKA_URL_DOCKER')
else:
    GeneralConfig.KAFKA_URL = getenv('KAFKA_URL_NO_DOCKER')
