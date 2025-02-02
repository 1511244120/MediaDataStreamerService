import time
import json
import random
from datetime import datetime
from typing import List, Dict, Any
from kafka import KafkaProducer
from faker import Faker
class Producer:
    def __init__(self, kafka_servers: List[str], topic: str) -> None:
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.fake = Faker()
        self.hashtags = ['#data', '#AI', '#BigData', '#Kafka', '#Python', '#project',
                         '#developer', '#technology']
    def generate_social_media_data(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "user_id": random.randint(1, 1000),
            "track_id": random.randint(1, 50000),
            "timestamp": self.fake.date_time_this_month().isoformat(),
            "event_type": random.choices(["streaming", "like"],
                                         weights=[0.9, 0.1],
                                         k=1)[0]
        }
        return data
    def send_data(self, data: Dict[str, Any]) -> None:
        self.producer.send(self.topic, data)
        self.producer.flush()
    def run(self, interval: float = 1.0) -> None:
        try:
            while True:
                data = self.generate_social_media_data()
                print(f"Generated social media data: {data}")
                self.send_data(data)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Stopping data generator.")
        finally:
            self.producer.close()
if __name__ == '__main__':
    kafka_servers = ['kafka:9092']  # Kafka broker address
    topic = 'social_media_data'  # Topic to send data to
    data_generator = Producer(kafka_servers, topic)
    data_generator.run(interval=1.0) # Generate data every second