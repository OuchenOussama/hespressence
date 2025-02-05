from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from ..config.settings import Config
import logging

class FlinkManager:
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.enable_checkpointing(Config.FLINK.checkpoint_interval)
        self.env.get_checkpoint_config().set_min_pause_between_checkpoints(
            Config.FLINK.min_pause_between_checkpoints
        )
        
        self.settings = EnvironmentSettings.new_instance()\
            .in_streaming_mode()\
            .build()
        self.t_env = StreamTableEnvironment.create(
            self.env, 
            environment_settings=self.settings
        )

    def create_kafka_source(self):
        return self.t_env.execute_sql("""
            CREATE TABLE kafka_comments (
                id STRING,
                user_comment STRING,
                topic STRING,
                article_title STRING,
                article_url STRING,
                score INT,
                created_at TIMESTAMP(3),
                proctime AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'hespress.comments.*',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'flink_consumer_group',
                'format' = 'json'
            )
        """)

    def create_postgres_sink(self):
        logging.info("Creating processed_comments table in PostgreSQL.")
        try:
            self.t_env.execute_sql("""
                CREATE TABLE IF NOT EXISTS processed_comments (
                    id STRING,
                    article_title STRING,
                    user_comment STRING,
                    topic STRING,
                    score DOUBLE,
                    created_at TIMESTAMP(3),
                    stored_at TIMESTAMP(3),
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'hostname' = 'postgres',
                    'port' = '5432',
                    'database-name' = 'hespress_db',
                    'username' = 'postgres',
                    'password' = 'postgres'
                )
            """)
            logging.info("Table processed_comments created successfully.")
        except Exception as e:
            logging.error(f"Error creating processed_comments table: {str(e)}")
            raise e 