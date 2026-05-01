# Debezium PostgreSQL → Kafka CDC + Мониторинг

## Предварительные требования

- Docker и Docker Compose
- Python 3.8+ (для потребителя)

## Запуск

1. Клонируйте репозиторий и перейдите в папку проекта.
2. Запустите все сервисы:

```bash
docker-compose up -d

Настройка Debezium Connector
Отправьте конфигурацию коннектора в Kafka Connect (порт REST API 8084):

bash
curl -X POST -H "Content-Type: application/json" -d @kafka-connect/debezium-connector-config.json http://localhost:8084/connectors

Получение данных из Kafka (Python)
Установите зависимости (если используете прямой consumer):

bash
pip install kafka-python
Запустите потребитель:

bash
python consumer/consumer.py
Если прямой consumer не работает, используйте альтернативный скрипт через subprocess:

bash
python consumer/consumer_safe.py
Вы увидите сообщения из топиков postgres.public.users и postgres.public.orders.

Мониторинг (Prometheus + Grafana)
Prometheus – сбор метрик: http://localhost:9091

Grafana – визуализация: http://localhost:3002 (логин admin / admin)

Источник данных в Grafana
Добавьте Prometheus: URL http://prometheus:9090 (имя сервиса внутри Docker).

Метрики
JMX Exporter (порт 9080) экспортирует метрики Kafka Connect.
В Prometheus цель jmx-exporter должна быть UP.

Пример запроса в Grafana: jmx_config_reload_failure_total (или kafka_connect_task_records_in_count, если доступны).

Остановка
bash
docker-compose down -v