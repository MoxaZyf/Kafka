import json
from kafka import KafkaProducer
import sys
import os

BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
producer = KafkaProducer(bootstrap_servers=BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

if len(sys.argv) < 2:
    print("Usage:")
    print("  python admin.py block <user> <blocked_user>")
    print("  python admin.py unblock <user> <blocked_user>")
    print("  python admin.py add_word <word>")
    print("  python admin.py remove_word <word>")
    sys.exit(1)

cmd = sys.argv[1]
if cmd == 'block':
    _, _, user, blocked = sys.argv
    producer.send('blocked_users', {'user_id': user, 'blocked_user': blocked, 'action': 'block'})
    print(f"Sent block: {user} -> {blocked}")
elif cmd == 'unblock':
    _, _, user, blocked = sys.argv
    producer.send('blocked_users', {'user_id': user, 'blocked_user': blocked, 'action': 'unblock'})
    print(f"Sent unblock: {user} -> {blocked}")
elif cmd == 'add_word':
    _, _, word = sys.argv
    producer.send('banned_words', {'word': word, 'action': 'add'})
    print(f"Sent add word: {word}")
elif cmd == 'remove_word':
    _, _, word = sys.argv
    producer.send('banned_words', {'word': word, 'action': 'remove'})
    print(f"Sent remove word: {word}")
else:
    print("Unknown command")