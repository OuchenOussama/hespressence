from datetime import time
import threading

from src.processing.flink_manager import FlinkManager
from src.scraper.kafka_producer import HespressKafkaProducer
from src.storage.mongo_consumer import MongoDBConsumer

def run_producer():
    producer = HespressKafkaProducer()
    while True:
        producer.produce_comments()
        time.sleep(300)  # Run every 5 minutes

def run_mongo_consumer():
    consumer = MongoDBConsumer()
    consumer.consume_and_store()

def run_flink_processor():
    processor = FlinkManager()
    processor.create_kafka_source()
    processor.create_mongo_source()
    processor.create_postgres_sink()
    processor.env.execute("Hespress Comments Processing")

if __name__ == "__main__":
    # Start all components in separate threads
    producer_thread = threading.Thread(target=run_producer)
    mongo_thread = threading.Thread(target=run_mongo_consumer)
    flink_thread = threading.Thread(target=run_flink_processor)
    
    producer_thread.start()
    mongo_thread.start()
    flink_thread.start() 