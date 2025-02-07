from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from ..config.settings import Config
from kafka.errors import NoBrokersAvailable
import time
import logging
from datetime import datetime
from .flink_processor import FlinkProcessor
import concurrent.futures
from tensorflow.keras.models import load_model
import numpy as np
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pickle

class MongoDBConsumer:
    def __init__(self, max_retries=3):
        self.client = None
        self.consumer = None
        self.connect_mongo_with_retry(max_retries)
        self.connect_kafka_with_retry(max_retries)
        self.flink_processor = FlinkProcessor()
        self.shutdown_flag = False

        # Load Sentiment Analysis Model
        self.model = load_model("src/model/model.h5")

        # Load Tokenizer (must be the same used for training)
        with open("src/model/tokenizer.pickle", "rb") as handle:
            self.tokenizer = pickle.load(handle)

        self.max_length = 100  # Adjust based on training

        # Define sentiment classes
        self.sentiment_classes = ["negative", "neutral", "positive"]

    def predict_sentiment(self, text):
        """Predict sentiment of a given text using the trained model."""
        sequences = self.tokenizer.texts_to_sequences([text])
        padded_sequences = pad_sequences(sequences, maxlen=self.max_length)

        prediction = self.model.predict(padded_sequences)
        sentiment_index = np.argmax(prediction)  # Get class with highest probability
        return self.sentiment_classes[sentiment_index]

    def _save_to_mongo(self, comments):
        """Preprocess comments, predict sentiment, and save to MongoDB."""
        comments_to_insert = []
        for comment in comments:
            sentiment = self.predict_sentiment(comment.get('comment', ""))
            comment_data = {
                'id': comment.get('id'),
                'article_title': comment.get('article_title'),
                'article_url': comment.get('article_url'),
                'user_comment': comment.get('comment'),
                'topic': comment.get('topic'),
                'score': comment.get('score'),
                'created_at': comment.get('created_at'),
                'sentiment': sentiment  # Append sentiment prediction
            }
            self.comments_collection.update_one(
                {'id': comment_data['id']},
                {'$set': comment_data},
                upsert=True
            )
        logging.info(f"Upserted {len(comments)} comments into MongoDB with sentiment analysis")
        return len(comments)
