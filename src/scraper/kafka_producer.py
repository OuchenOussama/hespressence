import csv
from datetime import datetime
import feedparser
import time
import logging
import os
import json
from kafka import KafkaProducer

from .scraper_rss import HespressScraper

class HespressDataCollector:
    def __init__(self):
        self.scraper = HespressScraper()
        self.output_dir = 'data'
        self.ensure_output_dir()
        self.connect_with_retry(3)

    def ensure_output_dir(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def save_comments_to_csv(self, comments, filename):
        filepath = os.path.join(self.output_dir, filename)
        file_exists = os.path.exists(filepath)
        
        fieldnames = [
            'id',
            'article_title',
            'article_url',
            'comment',
            'topic',
            'score'
        ]
        
        with open(filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for comment in comments:
                comment_data = {
                    'id': comment.get('id'),
                    'article_title': comment.get('article_title'),
                    'article_url': comment.get('article_url'),
                    'comment': comment.get('comment'),
                    'topic': comment.get('topic'),
                    'score': comment.get('score')
                }
                writer.writerow(comment_data)

    def collect_article_comments(self, article):
        comments = self.scraper.get_comments(article['url'], article['title'])
        if comments:
            filename = f"comments_{datetime.now().strftime('%Y%m%d')}.csv"
            self.save_comments_to_csv(comments, filename)
            logging.info(f"Saved {len(comments)} comments for article: {article['title']}")

    def collect_comments(self):
        try:
            feed = feedparser.parse('https://www.hespress.com/feed')
            
            for entry in feed.entries:
                article = {
                    'url': entry.link,
                    'title': entry.title.replace('"', '').replace('"', '').replace('"', '').strip()
                }
                self.collect_article_comments(article)
                time.sleep(1)  # Be nice to the server
                
        except Exception as e:
            logging.error(f"Error collecting comments: {str(e)}")

    def connect_with_retry(self, max_retries):
        retries = 0
        while retries < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=['kafka:9092'],
                    api_version=(0, 10, 1),
                    request_timeout_ms=30000,
                    max_block_ms=30000,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logging.info("Successfully connected to Kafka")
                return
            except Exception as e:
                retries += 1
                logging.error(f"Failed to connect to Kafka, retrying in 5 seconds... ({retries}/{max_retries})")
                logging.error(f"Error: {str(e)}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka after maximum retries")