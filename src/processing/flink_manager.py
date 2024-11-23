from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from ..config.settings import Config
from ..utils.logger import get_logger

logger = get_logger(__name__)

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
                comment STRING,
                topic STRING,
                article_title STRING,
                article_url STRING,
                score INT,
                timestamp TIMESTAMP(3),
                proctime AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'hespress.comments.*',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'flink_consumer_group',
                'format' = 'json'
            )
        """)

    def create_mongo_source(self):
        return self.t_env.execute_sql("""
            CREATE TABLE mongodb_comments (
                id STRING,
                comment STRING,
                topic STRING,
                article_title STRING,
                article_url STRING,
                score INT,
                timestamp TIMESTAMP(3)
            ) WITH (
                'connector' = 'mongodb',
                'uri' = 'mongodb://localhost:27017',
                'database' = 'hespress_db',
                'collection' = 'comments'
            )
        """)

    def create_postgres_sink(self):
        return self.t_env.execute_sql("""
            CREATE TABLE processed_comments (
                id STRING,
                comment STRING,
                topic STRING,
                score INT,
                processed_timestamp TIMESTAMP(3),
                processing_type STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://localhost:5432/hespress_db',
                'table-name' = 'processed_comments',
                'username' = 'postgres',
                'password' = 'postgres'
            )
        """) 