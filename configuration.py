from datetime import datetime, timezone
from typing import List
import yaml


class ConfigurationModel:
    def __init__(self):
        self.mongo_config: MongoConfigurationModel = None
        self.sources: int = None
        self.attributes: List[str] = None
        self.measurements_in_block: int = None
        self.time_line: TimeLineModel = None
        self.values: ValuesModel = None


class MongoConfigurationModel:
    def __init__(self):
        self.uri: str = None
        self.database: str = None
        self.input_collection: str = None
        self.output_collection: str = None


class TimeLineModel:
    def __init__(self):
        self.date_from: datetime = None
        self.date_to: datetime = None


class ValuesModel:
    def __init__(self):
        self.minimum: float = None
        self.q1: float = None
        self.median: float = None
        self.q3: float = None
        self.maximum: float = None


class ConfigurationParser:
    @staticmethod
    def parse_from_file(filename: str) -> ConfigurationModel:
        config = yaml.load(open(filename, 'r', encoding='utf-8'))['configuration']
        config_model = ConfigurationModel()
        config_model.mongo_config = ConfigurationParser.__parse_mongo_config(config['mongo'])
        config_model.sources = config['sources']
        config_model.attributes = config['attributes']
        config_model.measurements_in_block = config['measurements_in_block']
        config_model.time_line = ConfigurationParser.__parse_time_line(config['time_line'])
        config_model.values = ConfigurationParser.__parse_values(config['values'])
        return config_model

    @staticmethod
    def __parse_mongo_config(config_elem: dict) -> MongoConfigurationModel:
        config = MongoConfigurationModel()
        config.uri = config_elem['uri']
        config.database = config_elem['database']
        config.input_collection = config_elem['input_collection']
        config.output_collection = config_elem['output_collection']
        return config

    @staticmethod
    def __parse_time_line(config_time_line: dict) -> TimeLineModel:
        config = TimeLineModel()
        config.date_from = config_time_line['from']
        config.date_to = config_time_line['to']
        config.date_from = datetime.combine(config.date_from, datetime.min.time()).replace(tzinfo=timezone.utc)
        config.date_to = datetime.combine(config.date_to, datetime.min.time()).replace(tzinfo=timezone.utc)
        return config

    @staticmethod
    def __parse_values(config_values: dict) -> ValuesModel:
        config = ValuesModel()
        config.minimum = config_values['minimum']
        config.q1 = config_values['q1']
        config.median = config_values['median']
        config.q3 = config_values['q3']
        config.maximum = config_values['maximum']
        return config
