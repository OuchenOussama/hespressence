from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from ..config.settings import Config
from kafka.errors import NoBrokersAvailable
import time

class MongoDBConsumer:
    def __init__(self, max_retries=3):
        self.connect_with_retry(max_retries)

    def connect_with_retry(self, max_retries):
        for i in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    bootstrap_servers=Config.KAFKA.bootstrap_servers,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='earliest',
                    group_id='mongo_consumer_group'
                )
                # Connect to MongoDB
                self.client = MongoClient(Config.MONGODB.uri)
                self.db = self.client[Config.MONGODB.database]
                self.comments_collection = self.db['comments']
                
                # Subscribe to topics
                topic_pattern = f'^{Config.KAFKA.topics_prefix}.*'
                self.consumer.subscribe(pattern=topic_pattern)
                return
            except NoBrokersAvailable:
                if i == max_retries - 1:
                    raise
                print(f"Failed to connect to Kafka, retrying in 5 seconds... ({i+1}/{max_retries})")
                time.sleep(5)

    def consume_and_store(self):
        try:
            for message in self.consumer:
                comment = message.value
                
                # Update or insert the comment based on its ID
                self.comments_collection.update_one(
                    {'id': comment['id']},
                    {'$set': comment},
                    upsert=True
                )
                
        except Exception as e:
            print(f"Error consuming messages: {str(e)}")
            raise e
