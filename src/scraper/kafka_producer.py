from kafka import KafkaProducer
import json
from datetime import datetime

from scraper.scraper_rss import HespressScraper
from ..config.settings import Config


class HespressKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.scraper = HespressScraper()

    def produce_article_comments(self, article):
        comments = self.scraper.get_comments(article['url'], article['title'])
        
        for comment in comments:
            topic_name = f"{Config.KAFKA.topics_prefix}.{comment['topic'].lower()}"
            comment['timestamp'] = datetime.now().isoformat()
            
            self.producer.send(topic_name, value=comment)

    def run(self):
        try:
            while True:
                articles = self.scraper.get_articles_from_website()
                for article in articles:
                    self.produce_article_comments(article)
                self.producer.flush()
        except Exception as e:
            raise e