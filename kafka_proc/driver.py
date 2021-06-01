"""
Классы драйверов кафки. Написанные с использованием разных библиотек


with KafkaAdmin() as ka:
    ka.create_topic(topics={'name': 'top_func4', 'num_partitions': 3, 'replication_factor': 3})
"""

from uuid import uuid4
from json import dumps as jsd, loads as jsl
import asyncio
from os import cpu_count
from multiprocessing import Process, Pipe
from inspect import iscoroutinefunction

from . import default_cfg

from kafka.admin import KafkaAdminClient as DefaultAdminKafka, NewTopic
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic as NewTopicConf


# Удалено! Работает плохо!
# from aiokafka import AIOKafkaConsumer
class AIOKafkaConsumer:
    pass


from GENERAL_CONFIG import GeneralConfig


class ErrorKafkaAdmin(Exception):
    pass


class KafkaAdmin:
    """
    Администрирование кафки через библиотеку kafka_proc-python
    1. Управление топиками
    """

    """
    Инициализация
    """

    def __init__(self, hosts=None, connection_option=None, confluent=False):
        """
        :param hosts:
        :param connection_option:
        """

        if hosts is None:
            self.hosts = GeneralConfig.KAFKA_URL.split(',')
        else:
            self.hosts = hosts

        if connection_option is None:
            self.connection_option = default_cfg.DEFAULT_CONNECTION_OPTION_ADMIN
        else:
            self.connection_option = connection_option

        self.confluent = confluent

    """
    Контекст
    """

    def __enter__(self):
        """
        Активация клиента админки
        :return:
        """

        if self.confluent:
            self.admin = AdminClient({
                "bootstrap.servers": ",".join(self.hosts)
            })
        else:
            try:
                self.admin = DefaultAdminKafka(
                    bootstrap_servers=self.hosts
                )
            except Exception as exp:
                # Админка конфлюента убога и не может выяснить сколько топиков
                raise ErrorKafkaAdmin(exp)

                # self.admin = AdminClient({
                #     "bootstrap.servers": ",".join(self.hosts)
                # })
                #
                # self.confluent = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрытие клиента админки

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """

        if self.confluent:
            pass
        else:
            self.admin.close()

    """
    Топики

    with KafkaAdmin() as ka:
        ka.create_topic(topics={'name': 'top_func4', 'num_partitions': 3, 'replication_factor': 3})    
    """

    def create_topic(self, topics, topic_global_option=None):
        """
        Создание топиков

        :param topics: list[dict={'name': 'top_func', 'num_partitions': 3, 'replication_factor': 3}] or simple dict
        class NewTopic

        :param topic_global_option: - create_topics add params
        :return:
        """

        if self.confluent:
            NewTopicClass = NewTopicConf
        else:
            NewTopicClass = NewTopic

        topic_to_kafka = []
        if isinstance(topics, list):
            for topic in topics:
                if topic.get('replication_factor') is None:
                    topic['replication_factor'] = len(self.hosts)

                if self.confluent:
                    topic['topic'] = topic.pop('name')
                topic_to_kafka.append(NewTopicClass(**topic))

        elif isinstance(topics, dict):
            if topics.get('replication_factor') is None:
                topics['replication_factor'] = len(self.hosts)
            if self.confluent:
                topics['topic'] = topics.pop('name')
            topic_to_kafka.append(NewTopicClass(**topics))
        else:
            raise ValueError('incorrect topics!')

        if self.confluent:
            result = self.admin.create_topics(topic_to_kafka)
        else:
            if topic_global_option is None:
                topic_global_option = {'validate_only': False,
                                       'timeout_ms': default_cfg.DEFAULT_BROKER_TIMEOUT_MS_OPERATIONS}

            result = self.admin.create_topics(new_topics=topic_to_kafka, **topic_global_option)
        return result

    def delete_topics(self, topics_name, timeout_ms=None):
        """
        Удаление топиков. НЕ ИСПОЛЬЗОВАТЬ!!!
        ВНИМАНИЕ! Удаление всех топиков привело к разрушению кластера.
        Докер просто не смог восстановить их. Пришлось создавать заново.
        Предварительно ВАЖНО очистить всё, что в волюмах лежит.
        Кароче вообще не работает. Умирает контейнер и всё!!!
        Без очистки волюмов ничего не работает.

        :param topics_name: - список имен тем через запятую
        :param timeout_ms: - время ожидания ответа от брокеров
        :return:
        """

        raise ValueError('THIS MAY BE DESTROY YOUR SYSTEM')

        if timeout_ms is None:
            timeout_ms = default_cfg.DEFAULT_BROKER_TIMEOUT_MS_OPERATIONS

        result = self.admin.delete_topics(topics=topics_name, timeout_ms=timeout_ms)
        return result

    def get_topics(self):
        if self.confluent:
            return None
        else:
            return self.admin.list_topics()


class KafkaProducerConfluent:
    """
    Продюсер (Производитель). confluent_kafka
    """

    """
    Инициализация
    """

    def __init__(
        self,
        hosts=None,
        configuration=None,
        use_tx=False,
        one_topic_name=None,
        auto_flush_size=0,
        flush_is_bad=False
    ):
        """

        :param configuration:
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        """

        if configuration is None:
            self.configuration = {
                'client.id': default_cfg.DEFAULT_CONNECTION_OPTION_ADMIN['client_id'],
                'socket.timeout.ms': default_cfg.DEFAULT_BROKER_TIMEOUT_MS_OPERATIONS
            }

            if use_tx:
                self.configuration['transactional.id'] = str(uuid4())
        else:
            self.configuration = configuration

        if hosts:
            self.configuration['bootstrap.servers'] = hosts
        else:
            if not self.configuration.get('bootstrap.servers'):
                self.configuration['bootstrap.servers'] = GeneralConfig.KAFKA_URL

        self.use_tx = use_tx
        self.topic_part_itr = None
        self.topic_parts = None
        self.one_topic_name = one_topic_name

        if auto_flush_size:
            self.auto_flush = True
        else:
            self.auto_flush = False

        self.auto_flush_size = auto_flush_size
        self.auto_flush_itr = 0
        self.flush_is_bad = flush_is_bad
    """
    Контекст
    """

    def __enter__(self):

        self.auto_flush_itr = 0
        self.producer = Producer(self.configuration)
        self.update_partition_settings(name_topic=self.one_topic_name)

        if self.use_tx:
            try:
                self.producer.abort_transaction(default_cfg.DEFAULT_TRANSACTION_TIMEOUT_SEC)
            except Exception:
                pass

            self.producer.init_transactions(default_cfg.DEFAULT_TRANSACTION_TIMEOUT_SEC)
            self.producer.begin_transaction()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        После выхода

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """

        self.auto_flush_itr = 0
        if self.use_tx:
            if exc_type:
                self.producer.abort_transaction()
            else:
                # flush вызывается под капотом commit_transaction
                self.producer.commit_transaction(default_cfg.DEFAULT_TRANSACTION_TIMEOUT_SEC)
        else:
            self.producer.flush(default_cfg.DEFAULT_FLUSH_TIMER_SEC)

        del self

    """
    Вспомогательные операции
    """

    def get_list_topics(self):
        """
        Все топики
        :return:
        """
        try:
            res = self.producer.list_topics().topics
            return res
        except Exception:
            return None

    def get_one_topic(self, name):
        """
        Один топик по имени
        :param name:
        :return:
        """
        try:
            res = self.producer.list_topics(topic=name).topics
            return res
        except Exception:
            return None

    def update_partition_settings(self, name_topic=None):
        """
        Обновить настройки партиций всех топиков

        :param name_topic: - либо конкретного топика
        :return:
        """

        if self.topic_parts is None:
            self.topic_part_itr = {}
            self.topic_parts = {}

        if name_topic is None:
            topics = self.get_list_topics()
        else:
            if self.topic_parts.get(name_topic) is not None:
                self.topic_parts.pop(name_topic)

            topics = self.get_one_topic(name_topic)

        for name, topic_obj in topics.items():
            list_partitions = list(topic_obj.partitions)
            if len(list_partitions) <= 1:
                continue

            self.topic_parts[name] = list_partitions
            self.topic_part_itr[name] = 0

    def put_data(self, key, value, topic=None, callback=None, partition=None):
        """
        Поместить данные в очередь на обработку для брокера сообщений
        Чтобы не думать об этом - дампим в строку джсона сразу. Имя топика и ключа - строго строкой

        :param key: - ключ сообщения. Сделать пустым если исползуется автопопил сообщений средствами кафки
        :param value: - значение сообщения

        :param topic: - имя топика - если не задано -то будет применяться имя основного топика self.one_topic_name
        :param partition: - раздел топика(число). если не указано - то балансировка нагрузки по разделам

        :param callback: func(err, msg): if err is not None...
        :return:
        """

        if topic is None and self.one_topic_name is None:
            raise AttributeError('NEED TOPIC NAME!')

        if topic is None:
            topic = self.one_topic_name

        dict_args = {
            'topic': str(topic),
            'value': jsd(value),
        }

        if key:
            dict_args['key']: str(key)

        if callback:
            dict_args['callback'] = callback

        if partition:
            # Прямое задание позиции

            dict_args['partition'] = partition
        else:
            # Смещение позиции равномерно

            top_name = dict_args['topic']
            topic_parts = self.topic_parts.get(top_name)
            if topic_parts:

                current_position = self.topic_part_itr[top_name]

                if key:
                    # Партиция нужна если есть ключ
                    dict_args['partition'] = topic_parts[current_position]

                current_position += 1
                if current_position >= len(topic_parts):
                    current_position = 0

                self.topic_part_itr[top_name] = current_position

        if self.auto_flush:
            # Авто-ожидание приёма буфера сообщений - третья версия

            self.producer.produce(**dict_args)

            self.auto_flush_itr = self.auto_flush_itr + 1
            if self.auto_flush_itr >= self.auto_flush_size:
                self.auto_flush_itr = 0
                self.producer.flush(default_cfg.DEFAULT_FLUSH_TIMER_SEC)
        else:
            if self.flush_is_bad:
                # Вторая версия алгоритма - флушить по факту
                try:
                    self.producer.produce(**dict_args)
                    self.producer.poll(0)
                except BufferError:
                    #  Дожидаемся когда кафка разгребёт очередь
                    self.producer.flush(default_cfg.DEFAULT_FLUSH_TIMER_SEC)
            else:
                # Первая версия
                self.producer.produce(**dict_args)
                self.producer.poll(0)


class KafkaConsumer:
    """
    Consumer (Потребитель).
    Есть две библиотеки - одна асинхронная (aiokafka), другая Confluent
    Идея - лёгким движением руки переключаться между ними.
    Чтобы была возможность гибко масштабироваться и выбирать наиболее устойчивый вариант потребителя
    """

    """
    Инициализация
    """

    def __init__(
            self,
            topic_name,
            msg_processor,
            hosts=None,
            # async_core=False,
            async_task_process=False,
            auto_commit_ms=None,
            batch_commit_size=None,
            group_id=None,
            one_client=None,
            auto_offset='earliest',
            configuration=None,
    ):
        """

        :param topic_name: - один топик строкой или несколько топиков списком.
        :param msg_processor: - def msg_processor(consumer, id_thread, msgs, **kwargs). Обработчик сообщений
        :param hosts: - Адреса брокера строкой через запятую, если не указаны - то по умолчанию

        # :param async_core: - выбор варианта чтения топиков True асинхронный/False синхронный - FALSE ONLY!
        :param async_task_process: - обработка данных в отдельном асинхронном планировщике

        :param auto_commit_ms: задать, если необходимо автоматически коммитить прочитанные сообщения

        :param batch_commit_size: количество сообщений в пакете, после обработки которых
        необходимо сделать ручной синхронный коммит. Асинхронные коммиты не нравятся даже самим разработчикам
        всех этих радостей... Они говорят, что это сулит кучу проблем, хоть и работать может быстрее.
        Я нашёл баланс - батч коммиты.

        :param group_id: идентификатор группы пользователей - для многопроцессорного/многомашинного чтения.
        это ключ к масштабированию системы.

        Необходимо создавать столько потребителей одновременно - сколько партиций в топике, и назначать им
        один и тот же group_id. Например - 3 ядра процессора читают один топик одновременно
        (история с одним приложением).

        3 Приложения на разных машинах читают один топик одновременно (количество машин равно количеству партиций).

        Если задавать разный идентификатор группы - то это другая концепция. Каждый клиент хочет видеть новое сообщение.

        :param one_client: - Истина, если требуется всего один поток. Масштабирование на уровне ASGI
        Это когда ваше приложение запускается на одной машине несколько раз, а значит нет смысла надеяться на ядра
        процессора - их уже используют.

        :param auto_offset: latest - последний, earliest-ранний. С какого сообщения начать читать

        :param configuration: - задать свою конфигурацию (все параметры игнорируются)
        """

        if group_id is None:
            group_id = str(uuid4())

        if configuration is None:
            if async_core:
                # Асинхронная библиотека

                self.configuration = {
                    # 'bootstrap_servers': GeneralConfig.KAFKA_URL,
                    'session_timeout_ms': default_cfg.DEFAULT_BROKER_TIMEOUT_MS_OPERATIONS,
                }

                if auto_commit_ms:
                    self.configuration['enable_auto_commit'] = True
                    self.configuration['auto_commit_interval_ms'] = auto_commit_ms
                else:
                    self.configuration['enable_auto_commit'] = False

                self.configuration['group_id'] = group_id
                self.configuration['auto_offset_reset'] = auto_offset
            else:
                # Синхронная библиотека

                self.configuration = {
                    # 'bootstrap.servers': GeneralConfig.KAFKA_URL,
                    'socket.timeout.ms': default_cfg.DEFAULT_BROKER_TIMEOUT_MS_OPERATIONS,
                }

                if auto_commit_ms:
                    self.configuration['enable.auto.commit'] = True
                    self.configuration['auto.commit.interval.ms'] = auto_commit_ms
                else:
                    self.configuration['enable.auto.commit'] = False

                self.configuration['group.id'] = group_id
                self.configuration['auto.offset.reset'] = auto_offset
        else:
            # Конфигурация пользователя

            self.configuration = configuration

        if async_core:
            key_hosts = 'bootstrap_servers'
        else:
            key_hosts = 'bootstrap.servers'

        if not self.configuration.get(key_hosts):
            if hosts:
                self.configuration[key_hosts] = hosts
            else:
                self.configuration[key_hosts] = GeneralConfig.KAFKA_URL

        if isinstance(topic_name, str):
            self.topic_name = [topic_name]
        else:
            self.topic_name = topic_name

        if auto_commit_ms:
            self.auto_commit = True
        else:
            self.auto_commit = False

        # self.async_core = async_core
        self.async_core = False  # Всегда используем конфлюент (Есть конфликты с фаустом)

        self.async_task_process = async_task_process
        self.one_client = one_client
        self.batch_commit_size = batch_commit_size
        self.msg_processor = msg_processor
        self.partitions = {}
        self.consumers = {}

    """
    Контекст
    """

    def __enter__(self):
        """
        Инициализация потребителей.
        Получаем число партиций для раздела

        :return:
        """

        self.update_partitions_confluent()
        self.run_process_consumers()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        После выхода

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        del self

    """
    Предпусковые операции 
    """

    def update_partitions_confluent(self):
        """
        Получаем число разделов для каждого топика через продюсер библиотеки конфлюент.
        Почему так - потому что нам сначало надо узнать сколько потоков запустить -
        а потом уже рожать их.

        :return:
        """

        if self.async_core:

            conf = {
                'bootstrap.servers': self.configuration['bootstrap_servers'],
                'client.id': default_cfg.DEFAULT_CONNECTION_OPTION_ADMIN['client_id'],
                'socket.timeout.ms': self.configuration['session_timeout_ms']
            }
        else:
            conf = {
                'bootstrap.servers': self.configuration['bootstrap.servers'],
                'client.id': default_cfg.DEFAULT_CONNECTION_OPTION_ADMIN['client_id'],
                'socket.timeout.ms': self.configuration['socket.timeout.ms']
            }

        with KafkaProducerConfluent(one_topic_name=self.topic_name[0], configuration=conf) as prod:
            for top in self.topic_name:
                self.partitions[top] = len(prod.get_one_topic(name=top)[top].partitions)

    """
    Запуск и обработка потоков потребления
    """

    def run_process_consumers(self):
        """
        Запуск многопоточной обработки - потоков не более, чем ядер(точнее - потоков) процессора
        Краткое пояснение - если есть топики,
        которые состоят из огромного количества партиций - потоков больше чем ядер порождать не нужно.
        согласно документации, при стратегии по умолчанию - кафка равномерно распределит всё между потребителями
        :return:
        """

        if self.one_client:
            # Если клиент один - то всего один стрим
            self.looping_consumer(id=0)
        else:
            # По количеству ядер
            process_count = min(cpu_count() + 1, max(self.partitions.values()))
            process_list = []
            for id in range(process_count):
                proc = Process(target=self.looping_consumer, args=(id,))
                process_list.append(proc)
                proc.start()

            for proc in process_list:
                proc.join()

    def looping_consumer(self, id):
        """
        Асинхронный эвентор

        :param id:
        :return:
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.process_kafka_consumer(id))

    async def process_kafka_consumer(self, id):
        """
        Поток обработки потребления. Поток у нас полу-асинхронный. Тоесть - обработка сообщений синхронная
        Почему - потому что офф док советует. Чтобы избежать проблем с коммитами.
        Можно привести к асинхронному виду, но в офф доках между строк читается, что быстро не всегда хорошо.
        :param id:
        :return:
        """

        consumer = await self.create_consumer(id)
        if self.async_core:
            # AIO
            await self.process_aio(consumer, id)
        else:
            # Confluent
            await self.process_confluent(consumer, id)

    async def create_consumer(self, id):
        """
        Создание потребителя с подпиской на топик
        :return:
        """

        cons = self.consumers.get(id)
        if cons is not None:
            return cons

        if self.async_core:
            cons = AIOKafkaConsumer(
                *self.topic_name,
                loop=asyncio.get_event_loop(),
                **self.configuration
            )
            await cons.start()
        else:
            cons = Consumer(self.configuration)
            cons.subscribe(topics=self.topic_name)

        self.consumers[id] = cons
        return cons

    """
    AIO - асинхронный поток потребления 
    """

    async def process_aio(self, consumer, id, **kwargs):
        """
        Асинхронная обработка батчей с таймером. Я ИЗОБРЁЛ!! ИЗОБРЁЛ!!!
        И спустя пол дня переизобрёл во что-то нормальное

        Беда была в библиотеке. Не работает адекватно метод getmany.
        Точнее - он совсем не работает.
        Попытки сделать такой же метод ни к чему не привели, просто потому что
        цикл вечно висит когда нет данных - на моменте await.
        Возможно и в библиотеке висит там же.
        А смысл - собрать необходимый батч. Но если данных на батч не хватит - последний пакет мы не увидим

        :param id:
        :param consumer:
        :return:
        """

        try:
            if self.batch_commit_size:
                # Асинхронный батч
                magic_batch = 100

                if self.batch_commit_size > magic_batch:
                    # НЕ ТЯНЕТ ФУНКЦИЯ БОЛЬШЕ magic_batch батчей - косяк бибилотеки
                    # Попытк ипросто запросить нужное количество батчей по 100 пакетов ни к чему не привели
                    # Из чего мы делаем вывод что в многопоточном исполнении - всё плохо
                    # Тюльпан увядает. Если делать в цикле вызов к библиотечной функции.

                    batch_commit_size = magic_batch

                    # if self.batch_commit_size % magic_batch == 0:
                    #     add_cycle = 0
                    # else:
                    #     add_cycle = 1
                    #
                    # count_cycle = (self.batch_commit_size // magic_batch) + add_cycle
                    count_cycle = 1
                else:
                    batch_commit_size = self.batch_commit_size
                    count_cycle = 1

                task_part_processor = None
                parent_conn = None
                child_conn = None

                while True:

                    create_task = False
                    if task_part_processor is None:
                        create_task = True
                    elif task_part_processor.done() or task_part_processor.cancelled():
                        create_task = True

                    if create_task:
                        # Установка задачи на обработку батча

                        if task_part_processor:
                            # сброс предыдущей задачи
                            self.aio_queue_task_stop(task_part_processor)

                        parent_conn, child_conn, task_part_processor = await self.aio_queue_task_create(
                            consumer=consumer,
                            id=id,
                            async_task=self.async_task_process,
                            **kwargs
                        )

                    no_msg = True
                    for _ in range(count_cycle):

                        msg = await consumer.getmany(
                            timeout_ms=default_cfg.DEFAULT_CONSUMER_ASYNC_AWAIT_TIME_OUT_SEC,
                            max_records=batch_commit_size
                        )

                        if len(msg) == 0:
                            msg = await consumer.getmany(
                                timeout_ms=default_cfg.DEFAULT_CONSUMER_ASYNC_AWAIT_TIME_OUT_SEC,
                                max_records=1
                            )

                        if len(msg) > 0:
                            no_msg = False
                            child_conn.send(msg)

                    if no_msg:
                        continue

                    if self.async_task_process:
                        # обработка непосредственно в планировщике уже идёт - просто ждём когда закончиться
                        await task_part_processor
                        task_part_processor = self.aio_queue_task_stop(task_part_processor)
                    else:
                        # Вариант обработки данных с возможностью синхронных/асинхронных вызовов без планировщика
                        # Фактически мы убиваем планировщик и напрямую парсим очередь в этом потоке

                        task_part_processor = self.aio_queue_task_stop(task_part_processor)

                        await KafkaConsumer.aio_queue_data_processing(
                            consumer=consumer,
                            auto_commit=self.auto_commit,
                            pipe_parent_conn=parent_conn,
                            msg_processor=self.msg_processor,
                            id=id,
                            **kwargs
                        )

            else:
                # Обработка пакетов по одному - сразу же
                while True:
                    msg = await consumer.getone()

                    if self.async_task_process:
                        # очередь с планировщиком - для извращенцев

                        parent_conn, child_conn, task_part_processor = await self.aio_queue_task_create(
                            consumer=consumer,
                            id=id,
                            async_task=self.async_task_process,
                            **kwargs
                        )
                        child_conn.send(msg)

                        await task_part_processor
                        self.aio_queue_task_stop(task_part_processor)
                    else:
                        # прямая обработка

                        await KafkaConsumer.aio_msg_processing_data(
                            consumer=consumer,
                            auto_commit=self.auto_commit,
                            msg_processor=self.msg_processor,
                            list_data=[msg],
                            id=id,
                            **kwargs
                        )
        finally:
            await consumer.stop()

    async def aio_queue_task_create(self, consumer, id, async_task=None, **kwargs):
        """
        Установка задачи-таймера

        :param id:
        :param async_task: обработка таймером остановки (False) или непосредственно таймером прерывания (True)
        :param consumer:
        :return:
        """

        parent_conn, child_conn = Pipe()
        task_part_processor = asyncio.create_task(
            self.aio_task_part_processor(
                consumer=consumer,
                auto_commit=self.auto_commit,
                pipe_parent_conn=parent_conn,
                msg_processor=self.msg_processor,
                async_task=async_task,
                id=id,
                **kwargs
            )
        )

        return parent_conn, child_conn, task_part_processor

    @staticmethod
    def aio_queue_task_stop(task_part_processor):
        """
        Остановка задачи таймера

        :param task_part_processor:
        :return:
        """
        try:
            task_part_processor.cancel()
            del task_part_processor
        except Exception as exp:
            pass

        return None

    @staticmethod
    async def aio_task_part_processor(consumer, auto_commit, pipe_parent_conn, msg_processor, async_task, id, **kwargs):
        """
        Задача - таймер, по истечении которого будут обработаны пакеты из очереди по принципу:
        Сколько есть - столько и обработать. Тоесть батч собран не полностью т вместо 100 сообщений там 5, например
        В потоковой обрабокте эта штука помогает немедленно получать все последние пакеты.
        Помогает балансировать нагрузку. Ну например у нас батч 10 000 пакетов и мы хотим чтобы 10 000
        обрабатывалось не дольше секунды. И если вдруг такое не будет происходить -
        этот алгоритм сам выравняет нагрузку

        :param id:
        :param async_task:
        :param consumer:
        :param auto_commit:
        :param pipe_parent_conn:
        :param msg_processor:
        :return:
        """

        if not async_task:
            await asyncio.sleep(default_cfg.DEFAULT_CONSUMER_ASYNC_AWAIT_TIME_OUT_SEC * 1.1)

        await KafkaConsumer.aio_queue_data_processing(
            consumer=consumer,
            auto_commit=auto_commit,
            pipe_parent_conn=pipe_parent_conn,
            msg_processor=msg_processor,
            id=id,
            **kwargs
        )

    @staticmethod
    async def aio_queue_data_processing(consumer, auto_commit, pipe_parent_conn, msg_processor, id, **kwargs):
        """
        Обработка пакета данных из очереди

        :param id:
        :param consumer:
        :param auto_commit:
        :param pipe_parent_conn:
        :param msg_processor:
        :return:
        """

        list_data = []
        time_poll = 0
        while True:
            data_msg = await KafkaConsumer.aio_get_pipe_data(pipe_parent_conn, time_poll)
            if data_msg:
                for top, ld in data_msg.items():
                    list_data.append(ld)
            else:
                if len(list_data) == 0:
                    """
                    данных ещё нет - увеличить интервал, ждать
                    ОБЯЗАТЕЛЬНО await asyncio.sleep(time_poll) - это позволяет неявно переключить евентор, 
                    без этого никак работать не будет. Сопрограммы штука конечно мощная. 
                    но бесконечный цикл внутри сопрограммы может захватить власть над миром... 
                    Ну просто вы останетесь тут. И никакой вам конкурентности...
                    """

                    time_poll = 0.00001
                    await asyncio.sleep(time_poll)
                    continue
                else:
                    break

        # закрываем пайп
        pipe_parent_conn.close()

        if len(list_data) == 0:
            return True

        for ld in list_data:
            await KafkaConsumer.aio_msg_processing_data(
                consumer=consumer,
                auto_commit=auto_commit,
                msg_processor=msg_processor,
                list_data=ld,
                id=id,
                **kwargs
            )

        return True

    @staticmethod
    async def aio_get_pipe_data(pipe_parent_conn, time_poll_s):
        """
        Автоматический анализ очереди

        :param time_poll_s:
        :param pipe_parent_conn:
        :return:
        """
        max_repeat = 3

        for _ in range(max_repeat):
            res = pipe_parent_conn.poll(time_poll_s)
            if res:
                return pipe_parent_conn.recv()

        return None

    @staticmethod
    async def aio_msg_processing_data(consumer, auto_commit, msg_processor, list_data, id, **kwargs):
        """
        Обработка сообщений указанным процессором

        :param consumer:
        :param auto_commit:
        :param msg_processor:
        :param list_data:
        :param id:
        :param kwargs:
        :return:
        """
        try:
            if iscoroutinefunction(msg_processor):
                await msg_processor(consumer, id, KafkaConsumer.aio_msg_processing(list_data), **kwargs)
            else:
                msg_processor(consumer, id, KafkaConsumer.aio_msg_processing(list_data), **kwargs)

            if not auto_commit:
                await consumer.commit()

        except Exception as exp:
            pass

    @staticmethod
    def aio_msg_processing(list_data):
        """
        Конвертор данных в универсальный генератор
        :param list_data:
        :return:
        """

        return (
            {
                'topic': i.topic,
                'partition': i.partition,
                'offset': i.offset,
                'key': i.key.decode('utf-8'),
                'value': jsl(i.value.decode('utf-8'))
            }
            for i in list_data
        )

    """
    Синхронный поток потребления - confluent. 
    """

    async def process_confluent(self, consumer, id, **kwargs):
        """
        Ничего замысловатого - просто читаем по одному либо пачками.
        Но опять стандартный метод пачек подводит. И висит в ожидании.
        Обошли так: consumer.poll(timeout=default_cfg.DEFAULT_CONSUMER_ASYNC_AWAIT_TIME_OUT_SEC)

        Здесь точка осознания - что такое мультипроцессорное конкурентное программирование
        :param id:
        :param consumer:
        :return:
        """

        try:
            if self.batch_commit_size:
                itr_batch = 0
                msgs = []
                while True:
                    if self.batch_commit_size:
                        # Пакетная обработка

                        msg = consumer.poll(timeout=default_cfg.DEFAULT_CONSUMER_ASYNC_AWAIT_TIME_OUT_SEC)

                        process_data = False
                        msg_ok = True

                        if msg is None:
                            msg_ok = False
                            if len(msgs) == 0:
                                continue
                            else:
                                process_data = True
                        else:
                            if msg.error():
                                msg_ok = False
                                if len(msgs) == 0:
                                    continue
                                else:
                                    process_data = True

                        if msg_ok:
                            if self.async_task_process:
                                msg = KafkaConsumer.conf_one_msg_convertor(msg)

                            msgs.append(msg)
                            itr_batch += 1
                            if itr_batch < self.batch_commit_size:
                                continue

                            itr_batch = 0
                            process_data = True

                        if process_data:
                            if self.async_task_process:
                                # Конкурентный обработчик, чтобы не тормозить очередь
                                await self.conf_task_processor_create(consumer, msgs, id, **kwargs)
                            else:
                                # Прямая обработка
                                await KafkaConsumer.conf_msgs_processing(
                                    msg_processor=self.msg_processor,
                                    auto_commit=self.auto_commit,
                                    consumer=consumer,
                                    msgs=msgs,
                                    id=id,
                                    **kwargs
                                )
                            msgs.clear()

            else:
                while True:
                    # Разовая обработка
                    msg = consumer.poll()
                    if msg is None:
                        continue
                    if msg.error():
                        continue

                    if self.async_task_process:
                        msg = KafkaConsumer.conf_one_msg_convertor(msg)

                    msgs = [msg]

                    if self.async_task_process:
                        # Конкурентный обработчик, чтобы не тормозить очередь
                        await self.conf_task_processor_create(consumer, msgs, id, **kwargs)
                    else:
                        # Прямая обработка
                        await KafkaConsumer.conf_msgs_processing(
                            msg_processor=self.msg_processor,
                            auto_commit=self.auto_commit,
                            consumer=consumer,
                            msgs=msgs,
                            id=id,
                            **kwargs
                        )
        finally:
            consumer.close()

    async def conf_task_processor_create(self, consumer, msgs, id, **kwargs):
        """
        Постановка задачи на параллельную, точнее конкурентную обработку

        :param id:
        :param consumer:
        :param msgs:
        :return:
        """

        future = asyncio.create_task(
            self.conf_general_msg_proc_task(
                consumer=consumer,
                auto_commit=self.auto_commit,
                msgs=msgs,
                msg_processor=self.msg_processor,
                id=id,
                **kwargs
            )
        )

        await future

    @staticmethod
    async def conf_general_msg_proc_task(consumer, auto_commit, msgs, msg_processor, id, **kwargs):
        """
        Таск конкурентной/паралелльной обработки

        :param id:
        :param consumer:
        :param auto_commit:
        :param msgs:
        :param msg_processor:
        :return:
        """

        await KafkaConsumer.conf_msgs_processing(
            msg_processor=msg_processor,
            auto_commit=auto_commit,
            consumer=consumer,
            msgs=msgs,
            id=id,
            use_trf=False,
            **kwargs
        )

        return True

    @staticmethod
    async def conf_msgs_processing(msg_processor, auto_commit, consumer, msgs, id, use_trf=True, **kwargs):
        """
        Обработка сообщений

        :param id:
        :param use_trf:
        :param auto_commit:
        :param msg_processor:
        :param consumer:
        :param msgs:
        :return:
        """
        try:
            if use_trf:
                msgs_list = KafkaConsumer.conf_msg_processing(msgs)
            else:
                msgs_list = msgs

            if iscoroutinefunction(msg_processor):
                await msg_processor(consumer, id, msgs_list, **kwargs)
            else:
                msg_processor(consumer, id, msgs_list, **kwargs)

            if not auto_commit:
                consumer.commit()

        except Exception as exp:
            pass

    @staticmethod
    def conf_msg_processing(msgs):
        """
        Конвертор данных в универсальный генератор
        :param msgs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message
        :return:
        """
        return (
            KafkaConsumer.conf_one_msg_convertor(msg)
            for msg in msgs
        )

    @staticmethod
    def conf_one_msg_convertor(msg):
        return {
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': msg.key().decode('utf-8'),
            'value': jsl(msg.value().decode('utf-8'))
        }


"""
Пилотные тесты, которые были успешно пройдены 
"""
#
# list_val = []
# batch_number = 1
#
#
# async def msg_processing(consumer, id_thread, msgs, **kwargs):
#     global list_val, batch_number
#
#     for i in msgs:
#         val = i['value']['i']
#         list_val.append(val)
#
#     print(f'thread {id_thread}| batch №{batch_number}| get {len(list_val)} data')
#
#     batch_number = batch_number + 1
#
#
if __name__ == '__main__':
    imp_topic = 'WOW777'

    with KafkaAdmin() as ka:
        ka.create_topic({'name': imp_topic, 'num_partitions': 4, 'replication_factor': 3})

    # with KafkaProducerConfluent(use_tx=False, one_topic_name=imp_topic) as kp:
    #     for i in range(37):
    #         kp.put_data(key='JOAN', value={'i': i})

    # with KafkaConsumer(
    #         topic_name=imp_topic,
    #         msg_processor=msg_processing,
    #         batch_commit_size=1000,
    #         async_task_process=True,
    #         async_core=False) as cons:
    #     pass
