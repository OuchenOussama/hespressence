import signal
import sys
import threading
from time import sleep

from src.scraper.kafka_producer import HespressDataCollector
from src.scraper.wait_for_kafka import wait_for_kafka

def run_collector(collector):
    while True:
        collector.collect_comments()
        sleep(180)  # Run every 3 minutes

if __name__ == "__main__":
    wait_for_kafka(max_retries=15, retry_delay=15)
    
    collector = HespressDataCollector()
    
    # # Register signal handlers in the main thread
    # def signal_handler(sig, frame):
    #     collector.shutdown()  # Call shutdown on the collector
    #     sys.exit(0)

    # signal.signal(signal.SIGINT, signal_handler)
    # signal.signal(signal.SIGTERM, signal_handler)

    # Start collector in a separate thread
    collector_thread = threading.Thread(target=run_collector, args=(collector,))
    collector_thread.start()
    collector_thread.join()  # Wait for the collector thread to finish