from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from ..config.settings import Config
from kafka.errors import NoBrokersAvailable
import time
import logging
from datetime import datetime

class MongoDBConsumer:
    def __init__(self, max_retries=3):
        self.connect_mongo_with_retry(max_retries)
        self.connect_kafka_with_retry(max_retries)

    def connect_mongo_with_retry(self, max_retries):
        for i in range(max_retries):
            try:
                # Connect to MongoDB with increased timeout
                self.client = MongoClient(
                    Config.MONGO.uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                    waitQueueTimeoutMS=5000
                )
                self.db = self.client[Config.MONGO.database]
                self.comments_collection = self.db['comments']
                
                # Test the connection
                logging.info("Testing MongoDB connection...")
                self.client.admin.command('ping')
                logging.info("Successfully connected to MongoDB")
                
                # Setup Kafka consumer if needed
                if hasattr(Config.KAFKA, 'topics_prefix'):
                    self._setup_kafka_consumer()
                return
                
            except Exception as e:
                if i == max_retries - 1:
                    raise Exception(f"Failed to connect to MongoDB after {max_retries} attempts: {str(e)}")
                logging.warning(f"Failed to connect to MongoDB, retrying in 5 seconds... ({i+1}/{max_retries})")
                time.sleep(5)

    def connect_kafka_with_retry(self, max_retries):
        if not hasattr(Config.KAFKA, 'topics_prefix'):
            logging.info("Kafka topics_prefix not configured, skipping Kafka setup")
            return

        for i in range(max_retries):
            try:
                self._setup_kafka_consumer()
                logging.info("Successfully connected to Kafka")
                return
            except NoBrokersAvailable as e:
                if i == max_retries - 1:
                    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}")
                logging.warning(f"Failed to connect to Kafka, retrying in 5 seconds... ({i+1}/{max_retries})")
                time.sleep(5)

    def _setup_kafka_consumer(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=Config.KAFKA.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='mongo_consumer_group'
        )
        topic_pattern = f'^{Config.KAFKA.topics_prefix}.*'
        self.consumer.subscribe(pattern=topic_pattern)

    def save_comments(self, comments):
        """Save multiple comments to MongoDB."""
        try:
            comments_to_insert = []
            for comment in comments:
                comment_data = {
                    'id': comment.get('id'),
                    'article_title': comment.get('article_title'),
                    'article_url': comment.get('article_url'),
                    'comment': comment.get('comment'),
                    'topic': comment.get('topic'),
                    'score': comment.get('score'),
                    'created_at': datetime.now()
                }
                comments_to_insert.append(comment_data)
            
            if comments_to_insert:
                result = self.comments_collection.insert_many(comments_to_insert)
                logging.info(f"Inserted {len(result.inserted_ids)} comments into MongoDB")
        except Exception as e:
            logging.error(f"Error saving comments to MongoDB: {str(e)}")
            raise e
