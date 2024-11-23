from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from datetime import datetime
import feedparser
import time

from .scraper_rss import HespressScraper
from ..config.settings import Config


class HespressKafkaProducer:
    def __init__(self, max_retries=3):
        self.connect_with_retry(max_retries)
        self.scraper = HespressScraper()

    def connect_with_retry(self, max_retries):
        for i in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=Config.KAFKA.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                return
            except NoBrokersAvailable:
                if i == max_retries - 1:
                    raise
                print(f"Failed to connect to Kafka, retrying in 5 seconds... ({i+1}/{max_retries})")
                time.sleep(5)

    def produce_article_comments(self, article):
        comments = self.scraper.get_comments(article['url'], article['title'])
        
        for comment in comments:
            topic_name = f"{Config.KAFKA.topics_prefix}.{comment['topic'].lower()}"
            comment['timestamp'] = datetime.now().isoformat()
            
            self.producer.send(topic_name, value=comment)

    def produce_comments(self):
        feed = feedparser.parse('https://www.hespress.com/feed')
        
        for entry in feed.entries:
            article = {
                'url': entry.link,
                'title': entry.title.replace('"', '').replace('"', '').replace('"', '').strip()
            }
            self.produce_article_comments(article)
        
        self.producer.flush()