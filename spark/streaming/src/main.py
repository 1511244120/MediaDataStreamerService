import argparse
from streaming_pipeline import StreamingPipeline
from database_ops.db_manager import PostgresDataManager

DB_CONFIG = {
    'host': 'mediaDB',
    'database': 'media_data',
    'port': '5432',
    'user': 'test',
    'password': 'test1234'
}

KAFKA_CONFIG = {
    'bootstrap_servers': 'kafka:9092'
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka Streaming Pipeline Runner")
    parser.add_argument('--topic',
                        type=str,
                        required=True,
                        help='Kafka topic name')
    parser.add_argument('--app',
                        type=str,
                        required=True,
                        choices=['recommender', 'streaming'],
                        help='Type of app to run ("recommender" or "streaming")'
                        )

    args = parser.parse_args()

    db_manager = PostgresDataManager(DB_CONFIG)

    if args.app == 'streaming':
        # Create an instance of StreamingPipeline
        pipeline = StreamingPipeline(KAFKA_CONFIG, db_manager)
        try:
            pipeline.process_stream(topic=args.topic)
        except Exception as e:
            print(f"Error running streaming pipeline: {e}")
        finally:
            db_manager.close()