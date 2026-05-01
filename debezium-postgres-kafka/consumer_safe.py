import subprocess
import json
import sys

topics = ['postgres.public.users', 'postgres.public.orders']

for topic in topics:
    print(f"\n=== Сообщения из топика {topic} ===")
    # Запускаем консольный потребитель внутри контейнера Kafka
    cmd = [
        'docker', 'exec', '-i', 'debezium-postgres-kafka-kafka-1',
        'kafka-console-consumer',
        '--bootstrap-server', 'kafka:9092',
        '--topic', topic,
        '--from-beginning',
        '--max-messages', '10',
        '--timeout-ms', '5000'
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                if line:
                    try:
                        data = json.loads(line)
                        print(json.dumps(data, indent=2, ensure_ascii=False))
                    except:
                        print(line)
        else:
            print(f"Нет сообщений или ошибка: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("Таймаут при чтении топика")