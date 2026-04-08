import json
import os
import time
from kafka import KafkaConsumer, KafkaProducer
from threading import Thread, Lock
from kafka.errors import NoBrokersAvailable

BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
INPUT_TOPIC = 'messages'
OUTPUT_TOPIC = 'filtered_messages'
BLOCKED_TOPIC = 'blocked_users'
BANNED_TOPIC = 'banned_words'

blocked_users = {}
banned_words = set()
lock = Lock()

def wait_for_kafka():
    while True:
        try:
            # Пробуем создать продюсера для проверки соединения
            test_producer = KafkaProducer(bootstrap_servers=BROKER)
            test_producer.close()
            print("Kafka is ready")
            break
        except NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(5)

def load_data():
    global blocked_users, banned_words
    try:
        with open('/data/blocked.json', 'r') as f:
            blocked_users = json.load(f)
    except FileNotFoundError:
        blocked_users = {}
    try:
        with open('/data/banned.json', 'r') as f:
            banned_words = set(json.load(f))
    except FileNotFoundError:
        banned_words = set()

def save_data():
    with lock:
        with open('/data/blocked.json', 'w') as f:
            json.dump(blocked_users, f)
        with open('/data/banned.json', 'w') as f:
            json.dump(list(banned_words), f)

def censor(text):
    result = text
    for word in banned_words:
        result = result.replace(word, '*' * len(word))
    return result

def process_block_commands():
    consumer = KafkaConsumer(BLOCKED_TOPIC, bootstrap_servers=BROKER,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        data = msg.value
        user = data['user_id']
        blocked = data['blocked_user']
        action = data['action']
        with lock:
            if action == 'block':
                if user not in blocked_users:
                    blocked_users[user] = []
                if blocked not in blocked_users[user]:
                    blocked_users[user].append(blocked)
                    print(f"User {user} blocked {blocked}")
            elif action == 'unblock':
                if user in blocked_users and blocked in blocked_users[user]:
                    blocked_users[user].remove(blocked)
                    print(f"User {user} unblocked {blocked}")
        save_data()

def process_banned_commands():
    consumer = KafkaConsumer(BANNED_TOPIC, bootstrap_servers=BROKER,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        data = msg.value
        word = data['word']
        action = data['action']
        with lock:
            if action == 'add':
                banned_words.add(word)
                print(f"Added banned word: {word}")
            elif action == 'remove':
                banned_words.discard(word)
                print(f"Removed banned word: {word}")
        save_data()

def process_messages():
    consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=BROKER,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=BROKER,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for msg in consumer:
        data = msg.value
        sender = data['sender']
        receiver = data['receiver']
        content = data['content']
        
        with lock:
            if receiver in blocked_users and sender in blocked_users[receiver]:
                print(f"Message from {sender} to {receiver} blocked (user blocked)")
                continue
            censored_content = censor(content)
        
        output_msg = {
            'sender': sender,
            'receiver': receiver,
            'content': censored_content,
            'original_content': content
        }
        producer.send(OUTPUT_TOPIC, value=output_msg)
        print(f"Processed message from {sender} to {receiver}: '{censored_content}'")

if __name__ == '__main__':
    os.makedirs('/data', exist_ok=True)
    wait_for_kafka()
    load_data()
    
    t1 = Thread(target=process_block_commands, daemon=True)
    t2 = Thread(target=process_banned_commands, daemon=True)
    t1.start()
    t2.start()
    
    process_messages()