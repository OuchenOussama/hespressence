from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Table
from pyflink.common.typeinfo import Types
from ..config.settings import Config
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from ..storage.postgres_manager import PostgresManager

class FlinkProcessor:
    def __init__(self):
        # Initialize environments
        self._setup_environments()
        self._setup_sources_and_sinks()
        self.postgres_manager = PostgresManager()
        
    def _setup_environments(self):
        """Initialize Flink streaming and table environments"""
        self.stream_env = StreamExecutionEnvironment.get_execution_environment()
        self.stream_env.enable_checkpointing(Config.FLINK.checkpoint_interval)
        self.stream_env.get_checkpoint_config().set_min_pause_between_checkpoints(
            Config.FLINK.min_pause_between_checkpoints
        )
        
        self.table_env = StreamTableEnvironment.create(
            stream_execution_environment=self.stream_env,
            environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build()
        )

    def _setup_sources_and_sinks(self):
        """Setup Kafka source and PostgreSQL sink tables"""
        # Create Kafka source table for streaming
        self.table_env.execute_sql("""
            CREATE TABLE kafka_comments (
                id STRING,
                article_title STRING,
                article_url STRING,
                user_comment STRING,
                topic STRING,
                score DOUBLE,
                created_at TIMESTAMP(3),
                processing_time AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'hespress.comments',
                'properties.bootstrap.servers' = 'kafka:9092',
                'properties.group.id' = 'flink_consumer_group',
                'format' = 'json',
                'scan.startup.mode' = 'earliest-offset'
            )
        """)

        # Create PostgreSQL sink table
        self.table_env.execute_sql("""
            CREATE TABLE raw_comments (
                id STRING,
                article_title STRING,
                article_url STRING,
                user_comment STRING,
                topic STRING,
                score DOUBLE,
                created_at TIMESTAMP(3),
                stored_at TIMESTAMP(3),
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://postgres:5432/hespress_db',
                'table-name' = 'raw_comments',
                'username' = 'postgres',
                'password' = 'postgres'
            )
        """)

    def process_stream(self, comments: List[Dict[str, Any]]) -> None:
        """
        Real-time stream processing of comments and store in PostgreSQL
        """
        try:
            # Store raw comments in PostgreSQL
            self.postgres_manager.store_comments(comments)
            
            # Convert comments to DataStream
            comments_stream = self.stream_env.from_collection(
                collection=comments,
                type_info=Types.ROW_NAMED(
                    ['id', 'article_title', 'article_url', 'user_comment', 'topic', 'score', 'created_at'],
                    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), 
                     Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]
                )
            )
            
            # Create Table from DataStream
            comments_table = self.table_env.from_data_stream(comments_stream)
            
            # Insert into PostgreSQL using Flink SQL
            self.table_env.create_temporary_view("temp_comments", comments_table)
            
            insert_sql = """
                INSERT INTO raw_comments 
                SELECT 
                    id, article_title, article_url, user_comment, 
                    topic, score, created_at,
                    CURRENT_TIMESTAMP as stored_at
                FROM temp_comments
            """
            
            self.table_env.execute_sql(insert_sql)
            
        except Exception as e:
            logging.error(f"Error in stream processing: {str(e)}")
            raise e

    def _preprocess_comment(self, comment_row):
        """Prepare comment for NLP processing"""
        # TODO: Implement text preprocessing
        return comment_row

    def _apply_nlp_model(self, processed_comment):
        """Placeholder for NLP model application"""
        # TODO: Implement NLP model prediction
        pass

    def _post_process_prediction(self, prediction):
        """Process and format prediction results"""
        # TODO: Implement prediction post-processing
        pass

    def process_batch(self, comments: List[Dict[str, Any]]) -> None:
        """
        Batch processing for historical analysis or model retraining
        """
        try:
            # Convert comments to Table
            comments_table = self.table_env.from_elements(
                comments,
                ['id', 'article_title', 'article_url', 'user_comment', 'topic', 'score', 'created_at']
            )
            
            # Register for SQL querying
            self.table_env.create_temporary_view("comments_batch", comments_table)
            
            # Example batch analysis query
            # TODO: Customize based on specific batch processing needs
            analysis_result = self.table_env.sql_query("""
                SELECT 
                    topic,
                    COUNT(*) as comment_count,
                    AVG(score) as avg_score,
                    MIN(created_at) as earliest_comment,
                    MAX(created_at) as latest_comment
                FROM comments_batch
                GROUP BY topic
                ORDER BY comment_count DESC
            """)
            
            # Execute batch processing
            analysis_result.execute().print()
            
        except Exception as e:
            logging.error(f"Error in batch processing: {str(e)}")
            raise e

    def retrain_model(self, historical_data: Table):
        """
        Periodic batch processing for model retraining
        """
        # TODO: Implement model retraining logic
        pass