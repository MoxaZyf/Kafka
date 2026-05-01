from kafka import KafkaConsumer, TopicPartition
import json

bootstrap_servers = 'localhost:9092'
topics = ['postgres.public.users', 'postgres.public.orders']

consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    enable_auto_commit=False,
    group_id=None,
    auto_offset_reset='earliest'
)

for topic in topics:
    partitions = consumer.partitions_for_topic(topic)
    if partitions:
        for partition in partitions:
            tp = TopicPartition(topic, partition)
            consumer.assign([tp])
            consumer.seek_to_beginning(tp)
            pos = consumer.position(tp)
            print(f"Назначена партиция {topic}:{partition}, позиция начала: {pos}")
    else:
        print(f"Топик {topic} не имеет партиций")

print("Ожидание сообщений...")
while True:
    msgs = consumer.poll(timeout_ms=1000)
    if msgs:
        for tp, messages in msgs.items():
            for msg in messages:
                print(f"Топик: {msg.topic}, Партиция: {msg.partition}, Смещение: {msg.offset}")
                print("Значение:", msg.value.decode('utf-8'))
    else:
        # Если нет сообщений, но мы знаем, что они есть – возможно, проблема в том, что consumer.poll() не возвращает
        # Попробуем вывести for debug
        print("Нет сообщений за 1 секунду, продолжаем...")