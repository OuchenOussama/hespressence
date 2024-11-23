from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from ..config.settings import Config

class MongoDBConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=Config.KAFKA.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=Config.KAFKA.group_id,
            auto_offset_reset='earliest'
        )
        
        self.client = MongoClient(Config.MONGO.uri)
        self.db = self.client[Config.MONGO.database]
        self.collection = self.db[Config.MONGO.collection]

    def run(self):
        try:
            self.consumer.subscribe(pattern=f'{Config.KAFKA.topics_prefix}.*')
            
            for message in self.consumer:
                comment_data = message.value
                self.collection.insert_one(comment_data)
                
        except Exception as e:
            raise e
