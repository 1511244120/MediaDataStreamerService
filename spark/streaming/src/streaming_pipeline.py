from typing import Dict
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType,
                               TimestampType)
from db_manager import PostgresDataManager
from models.track_like_log import TrackLikeLog
from models.track_stream_log import TrackStreamLog

class StreamingPipeline:
    """
    A class to process Kafka streaming data using Spark Structured Streaming.
    """
    
    def __init__(self, kafka_config: Dict[str, str], db_manager: PostgresDataManager):
        """
        Initialize the streaming pipeline.
        
        :param kafka_config: A dictionary containing Kafka connection parameters.
        :param db_manager: An instance of PostgresDataManager for database operations.
        """
        self.spark = SparkSession.builder.appName("UnifiedStreamingPipeline").getOrCreate()
        self.kafka_config = kafka_config
        self.db_manager = db_manager
        
        # Define schemas for different log types
        self.streaming_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("track_id", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),  # Expected value: 'streaming'
        ])
        
        self.like_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("track_id", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),  # Expected value: 'like'
        ])

    def process_stream(self, topic: str) -> None:
        """
        Process streaming data from the specified Kafka topic.
        
        :param topic: Kafka topic name.
        """
        
        # Read data from Kafka topic as a streaming DataFrame
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers'])
            .option("subscribe", topic)
            .load()
        )
        
        # Parse Kafka message values as JSON and extract relevant data
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data").select(
            from_json(col("json_data"), self.streaming_schema).alias("streaming_data"),
            from_json(col("json_data"), self.like_schema).alias("like_data"),
        )
        
        # Filter and separate streaming and like data
        streaming_df = (
            parsed_df.select("streaming_data.*")
            .filter(col("streaming_data.event_type") == "streaming")
            .drop("event_type")  # Remove redundant column
        )
        
        like_df = (
            parsed_df.select("like_data.*")
            .filter(col("like_data.event_type") == "like")
            .drop("event_type")  # Remove redundant column
        )
        
        # Process and store each type of log
        streaming_df.writeStream.foreachBatch(self._process_streaming).outputMode("append").start()
        like_df.writeStream.foreachBatch(self._process_like).outputMode("append").start()
        
        # Await termination to keep the streams running
        self.spark.streams.awaitAnyTermination()

    def _process_streaming(self, batch_df, batch_id):
        """
        Process streaming logs and save them to `track_stream_log`.
        
        :param batch_df: DataFrame containing streaming logs.
        :param batch_id: Unique identifier for the streaming batch.
        """
        records = batch_df.collect()  # Collect batch data
        data = [record.asDict() for record in records]  # Convert records to dictionary format
        self.db_manager.insert(TrackStreamLog, data)  # Insert into database
        print(f"Processed {len(data)} streaming records into `track_stream_log`.")

    def _process_like(self, batch_df, batch_id):
        """
        Process like logs and save them to `track_like_log`.
        
        :param batch_df: DataFrame containing like logs.
        :param batch_id: Unique identifier for the like batch.
        """
        records = batch_df.collect()  # Collect batch data
        data = [record.asDict() for record in records]  # Convert records to dictionary format
        self.db_manager.insert(TrackLikeLog, data)  # Insert into database
        print(f"Processed {len(data)} like records into `track_like_log`.")