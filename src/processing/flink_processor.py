from .flink_manager import FlinkManager
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Table
from pyflink.common.typeinfo import Types
from ..config.settings import Config
import logging
from typing import List, Dict, Any
from ..storage.postgres_manager import PostgresManager

class FlinkProcessor:
    def __init__(self):
        # Initialize environments
        self._setup_environments()
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
        
        manager = FlinkManager()
        manager.create_kafka_source()
        manager.create_postgres_sink()

    def process_stream(self, comments: List[Dict[str, Any]]) -> None:
        """
        Real-time stream processing of comments and store in PostgreSQL
        """
        try:
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
            
            # Register for SQL querying
            self.table_env.create_temporary_view("temp_comments", comments_table)
            
            # Insert into PostgreSQL using Flink SQL
            insert_sql = """
                INSERT INTO processed_comments 
                SELECT 
                    id, article_title, article_url, user_comment, 
                    topic, score, created_at,
                    CURRENT_TIMESTAMP as stored_at
                FROM temp_comments
            """
            
            self.table_env.execute_sql(insert_sql)
            logging.info("Comments inserted into PostgreSQL successfully.")
            
        except Exception as e:
            logging.error(f"Error in stream processing: {str(e)}")
            raise e

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