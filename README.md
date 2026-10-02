# Сервис скоринга фродовых транзакций (MTS MLOps)

Сервис для потоковой обработки и скоринга транзакций из Kafka с использованием модели CatBoost.

## Архитектура
1. **Zookeeper & Kafka** — потоковая передача и обработка сообщений.
2. **Fraud Detector** — Python-сервис, который читает транзакции из топика `transactions`, выполняет препроцессинг данных, делает инференс CatBoost-модели на CPU и отправляет результат (`transaction_id`, `score`, `fraud_flag`) в топик `scoring`.

## Быстрый запуск

1. Клонировать этот репозиторий

2. Собрать и запустить контейнеры 
docker compose up --build -d

3. Запустить отправку сообщений в Kafka
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic transactions

4. Отправить тестовую транзакцию

{"transaction_id": "tx_1001", "hour": 14, "year": 2026, "month": 10, "day_of_month": 2, "day_of_week": 5, "gender_cat": 1, "merch_cat": 3, "cat_id_cat": 2, "one_city_cat": 0, "us_state_cat": 5, "jobs_cat": 12, "gender_cat_mean_enc": 0.45, "merch_cat_mean_enc": 0.12, "cat_id_cat_mean_enc": 0.08, "one_city_cat_mean_enc": 0.95, "us_state_cat_mean_enc": 0.33, "jobs_cat_mean_enc": 0.21, "hour_mean_enc": 0.15, "year_mean_enc": 0.02, "month_mean_enc": 0.05, "day_of_month_mean_enc": 0.03, "day_of_week_mean_enc": 0.04, "amount_log": 4.5, "population_city_log": 11.2, "distance_log": 2.1}

5. Чтобы посмотреть резуьтат
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic scoring --from-beginning

получим {"transaction_id": "tx_1001", "score": 0.0009169640915357603, "fraud_flag": 0}

все готово

