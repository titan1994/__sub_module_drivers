from __example.example_init import GeneralConfig

from kafka_proc.driver import KafkaAdmin
with KafkaAdmin() as ka:
    ka.create_topic(topics={'name': 'top_func444', 'num_partitions': 3, 'replication_factor': 3})


