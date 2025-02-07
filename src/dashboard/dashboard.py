# dashboard.py
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import pandas as pd
import time
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SentimentInsights:
    positive: Dict[str, float]  # {percentage: float, total: int}
    negative: Dict[str, float]
    neutral: Dict[str, float]

@dataclass
class TopicAnalysis:
    topic: str
    times_mentioned: int
    score_sum: float
    mean_score: float
    positive_count: int
    negative_count: int
    neutral_count: int

class DashboardApp:
    def __init__(self):
        self.app = Flask(__name__, static_folder='src/dashboard')
        self.setup_app()
        self.df: Optional[pd.DataFrame] = None
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.load_dataset()

    def setup_app(self) -> None:
        CORS(self.app)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        self.setup_routes()
        self.setup_socket_handlers()

    def load_dataset(self) -> None:
        try:
            csv_path = os.getenv('CSV_PATH', 'src/dashboard/hespress_dataset.csv')
            self.df = pd.read_csv(csv_path)
            self._validate_dataset()
        except Exception as e:
            logger.error(f"Failed to load dataset: {e}")
            self.df = None

    def _validate_dataset(self) -> None:
        required_columns = {'topic', 'sentiment', 'score', 'id'}
        if not all(col in self.df.columns for col in required_columns):
            missing = required_columns - set(self.df.columns)
            raise ValueError(f"Missing required columns: {missing}")

    def analyze_data(self) -> Tuple[List[Dict], Dict[str, Dict[str, float]]]:
        if self.df is None:
            raise ValueError("Dataset not loaded")

        # Simple grouping without multi-level columns
        topic_stats = self.df.groupby('topic').agg({
            'id': 'count',
            'score': ['sum', 'mean']
        }).reset_index()

        # Flatten column names
        topic_stats.columns = ['topic', 'count', 'score_sum', 'score_mean']
        
        # Sort by count in descending order
        topic_stats = topic_stats.sort_values('count', ascending=False)

        trending = []
        for _, row in topic_stats.iterrows():
            # Get sentiment counts for this topic
            topic_sentiments = self.df[self.df['topic'] == row['topic']]['sentiment'].value_counts()
            
            trending.append({
                'topic': str(row['topic']),
                'times_mentioned': int(row['count']),
                'score_sum': float(row['score_sum']),
                'mean_score': float(row['score_mean']),
                'positive_count': int(topic_sentiments.get('positive', 0)),
                'negative_count': int(topic_sentiments.get('negative', 0)),
                'neutral_count': int(topic_sentiments.get('neutral', 0))
            })

        # Calculate overall sentiment counts
        sentiment_counts = self.df['sentiment'].value_counts()
        total_count = len(self.df)
        
        insights = {}
        for sentiment in ['positive', 'negative', 'neutral']:
            count = int(sentiment_counts.get(sentiment, 0))
            insights[sentiment] = {
                'percentage': float(round((count / total_count) * 100, 2)),
                'total': count
            }

        return trending, insights

    def setup_routes(self) -> None:
        @self.app.route('/')
        def serve_dashboard():
            return send_from_directory(os.path.abspath("src/dashboard"), "index.html")
        
        @self.app.route('/visualization')
        def serve_d3():
            return send_from_directory(os.path.abspath("src/dashboard"), "visualization.js")

        @self.app.route("/api/sentiment", methods=["GET"])
        def get_sentiment_data():
            try:
                trending, insights = self.analyze_data()
                return jsonify({
                    "trending": trending,
                    "insights": insights
                })
            except Exception as e:
                logger.error(f"Error processing sentiment data: {e}")
                return jsonify({"error": str(e)}), 500

    def setup_socket_handlers(self) -> None:
        @self.socketio.on('connect')
        def handle_connect():
            logger.info("Client connected")
            self.executor.submit(self.process_data)

    def process_data(self) -> None:
        while True:
            try:
                trending, insights = self.analyze_data()
                self.socketio.emit("real_time_data", {
                    "trending": trending,
                    "insights": insights
                })
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error in data processing: {e}")
                time.sleep(1)

    def run(self, debug: bool = False) -> None:
        self.socketio.run(self.app, debug=debug)

if __name__ == "__main__":
    dashboard = DashboardApp()
    dashboard.run(debug=True)