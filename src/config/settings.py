from dataclasses import dataclass, field
from typing import List

@dataclass
class KafkaConfig:
    bootstrap_servers: List[str] = field(default_factory=lambda: ['kafka:9092'])
    topics_prefix: str = 'hespress.comments'
    group_id: str = 'hespress_group'

@dataclass
class MongoConfig:
    uri: str = 'mongodb://mongodb:27017'
    database: str = 'hespress_db'
    collection: str = 'comments'

@dataclass
class PostgresConfig:
    host: str = 'localhost'
    port: int = 5432
    database: str = 'hespress_db'
    user: str = 'postgres'
    password: str = 'postgres'

@dataclass
class FlinkConfig:
    checkpoint_dir: str = '/tmp/flink-checkpoints'
    checkpoint_interval: int = 5000
    min_pause_between_checkpoints: int = 500

class Config:
    KAFKA = KafkaConfig()
    MONGO = MongoConfig()
    POSTGRES = PostgresConfig()
    FLINK = FlinkConfig() 