import os
import json
import time
import signal
import sys
import logging
from catboost import CatBoostClassifier
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transactions")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "scoring")
GROUP_ID = os.getenv("GROUP_ID", "fraud_detector_group")
MODEL_PATH = os.getenv("MODEL_PATH", "my_catboost.cbm")
THRESHOLD = float(os.getenv("THRESHOLD", "0.98"))


class FraudModel:
    """Класс для инференса модели CatBoost."""

    def __init__(self, model_path: str, threshold: float):
        self.threshold = threshold
        self.model = CatBoostClassifier()
        logging.info(f"Загрузка модели из {model_path}...")
        self.model.load_model(model_path)
        self.feature_names = self.model.feature_names_
        logging.info(f"Модель успешно загружена. Признаки ({len(self.feature_names)}): {self.feature_names}")

    def predict(self, transaction: dict) -> tuple[float, int]:
        features = [transaction.get(feat, 0) for feat in self.feature_names]

        score = float(self.model.predict_proba([features])[0][1])
        fraud_flag = int(score >= self.threshold)

        return score, fraud_flag


class KafkaHandler:
    """Класс для устойчивого подключения и работы с Kafka."""

    def __init__(self, bootstrap_servers: str, input_topic: str, output_topic: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id

        self.producer = self._init_producer()
        self.consumer = self._init_consumer()

    @staticmethod
    def _safe_json_deserializer(payload: bytes):
        if payload is None:
            return None
        try:
            return json.loads(payload.decode("utf-8"))
        except Exception as e:
            logging.error(f"Ошибка десериализации JSON: {e}")
            return None

    def _init_producer(self) -> KafkaProducer:
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=3
                )
                logging.info("Kafka Producer успешно подключен.")
                return producer
            except Exception as e:
                logging.warning(f"Ошибка подключения к Producer ({e}). Повтор через 5 сек...")
                time.sleep(5)

    def _init_consumer(self) -> KafkaConsumer:
        while True:
            try:
                consumer = KafkaConsumer(
                    self.input_topic,
                    bootstrap_servers=self.bootstrap_servers,
                    value_deserializer=self._safe_json_deserializer,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    group_id=self.group_id
                )
                logging.info(f"Kafka Consumer подключен к топику '{self.input_topic}'.")
                return consumer
            except Exception as e:
                logging.warning(f"Ошибка подключения к Consumer ({e}). Повтор через 5 сек...")
                time.sleep(5)

    def send_score(self, payload: dict):
        self.producer.send(self.output_topic, value=payload)
        self.producer.flush()

    def close(self):
        logging.info("Закрытие соединений Kafka...")
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()


def main():
    model = FraudModel(model_path=MODEL_PATH, threshold=THRESHOLD)
    kafka = KafkaHandler(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        input_topic=INPUT_TOPIC,
        output_topic=OUTPUT_TOPIC,
        group_id=GROUP_ID
    )

    def shutdown_handler(signum, frame):
        logging.info("Получен сигнал завершения. Остановка сервиса...")
        kafka.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logging.info("Сервис запущен и готов к обработке сообщений...")

    for message in kafka.consumer:
        transaction = message.value
        if transaction is None:
            continue

        try:
            transaction_id = transaction.get("transaction_id")

            score, fraud_flag = model.predict(transaction)

            payload = {
                "transaction_id": transaction_id,
                "score": score,
                "fraud_flag": fraud_flag
            }

            kafka.send_score(payload)
            logging.info(f"Transaction ID: {transaction_id} | Score: {score:.4f} | Fraud: {fraud_flag}")

        except Exception as e:
            logging.error(f"Ошибка при обработке сообщения {transaction}: {e}")


if __name__ == "__main__":
    main()