import threading
from time import sleep

from src.scraper.kafka_producer import HespressDataCollector
from src.wait_for_kafka import wait_for_kafka

def run_collector():
    collector = HespressDataCollector()
    while True:
        collector.collect_comments()
        sleep(300)  # Run every 5 minutes

if __name__ == "__main__":
    wait_for_kafka(max_retries=15, retry_delay=15)
    # Start collector in a separate thread
    collector_thread = threading.Thread(target=run_collector)
    collector_thread.start()