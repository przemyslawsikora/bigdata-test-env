import json
import random
from argparse import ArgumentParser
from datetime import timedelta, datetime
from typing import List
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from configuration import ConfigurationParser, ConfigurationModel, TimeLineModel, MongoConfigurationModel


class DataProcessor:
    def __init__(self):
        self.client: MongoClient = None
        self.input_collection: Collection = None
        self.validation_table = None

    def process_data(self, config: ConfigurationModel, validation_file: str):
        start_time = datetime.now()
        day_list = self.__prepare_days_table(config.time_line)
        measurement_count = config.sources * len(config.attributes) * \
                            len(day_list) * (config.measurements_in_block * 4 + 3)
        print('Number of all measurements to send: {:,}'.format(measurement_count))

        self.__prepare_mongo(config.mongo_config)
        self.validation_table = {}
        for day in day_list:
            self.validation_table[day] = {}
            for s in range(1, config.sources + 1):
                source_name = '/data_sources/{:05}'.format(s)
                self.validation_table[day][source_name] = {}
                for attribute in config.attributes:
                    self.__init_validation_table_item(source_name, attribute, day)
                    docs = []
                    for value in [config.values.q1, config.values.median, config.values.q3]:
                        docs.append(self.__build_measurement_doc(source_name, attribute, day, value))
                        self.__update_validation_table_with_value(source_name, attribute, day, value)
                    self.input_collection.insert_many(docs)
                    for i in range(4):
                        self.__process_data_in_block(source_name, attribute, day, config)
                    self.__update_validation_table(source_name, attribute, day)
        assert self.input_collection.count_documents({}) == measurement_count, \
            'Number of documents that should be sent ({}) is not equal to the number of ' \
            'documents in the Mongo collection ({})'.format(self.input_collection.count_documents({}),
                                                            self.input_collection.count_documents({}))
        self.client.close()
        validation_table = {}
        for k, v in self.validation_table.items():
            validation_table[k.strftime('%Y-%m-%d')] = v
        with open(validation_file, 'w') as file:
            file.write(json.dumps(validation_table))
        elapsed = datetime.now() - start_time
        print("Program's execution taken {}".format(str(elapsed)))

    def __process_data_in_block(self, source: str, attribute: str, date_origin: datetime, config: ConfigurationModel):
        docs = []
        i = 0
        while i < config.measurements_in_block:
            value = random.uniform(config.values.minimum + 0.00001, config.values.maximum - 0.00001)
            self.__update_validation_table_with_value(source, attribute, date_origin, value)
            docs.append(self.__build_measurement_doc(source, attribute, date_origin, value))
            i = i + 1
            if i % 128 == 0:
                self.input_collection.insert_many(docs)
                docs = []
        if docs:
            self.input_collection.insert_many(docs)

    def __init_validation_table_item(self, source: str, attribute: str, date: datetime):
        self.validation_table[date][source][attribute] = {
            'count': 0,
            'minimum': float('inf'),
            'maximum': -float('inf'),
            'sum': 0,
            'mean': 0
        }

    def __update_validation_table_with_value(self, source: str, attribute: str, date: datetime, value: float):
        self.validation_table[date][source][attribute]['count'] += 1
        self.validation_table[date][source][attribute]['minimum'] = \
            min(self.validation_table[date][source][attribute]['minimum'], value)
        self.validation_table[date][source][attribute]['maximum'] = \
            max(self.validation_table[date][source][attribute]['maximum'], value)
        self.validation_table[date][source][attribute]['sum'] += value

    def __update_validation_table(self, source: str, attribute: str, date: datetime):
        self.validation_table[date][source][attribute]['mean'] = \
            self.validation_table[date][source][attribute]['sum'] / \
            self.validation_table[date][source][attribute]['count']

    def __prepare_mongo(self, mongo_config: MongoConfigurationModel):
        self.client = MongoClient(mongo_config.uri)
        db = self.client[mongo_config.database]
        db.drop_collection(mongo_config.input_collection)
        db.drop_collection(mongo_config.output_collection)
        self.input_collection = db[mongo_config.input_collection]
        self.input_collection.create_index('timestamp')
        self.input_collection.create_index([('source', ASCENDING), ('timestamp', ASCENDING), ('attribute', ASCENDING)])

    @staticmethod
    def __prepare_days_table(time_line: TimeLineModel) -> List[datetime]:
        current_day = time_line.date_from
        day_list = []
        while current_day < time_line.date_to:
            day_list.append(current_day)
            current_day = current_day + timedelta(days=1)
        return day_list

    @staticmethod
    def __build_measurement_doc(source: str, attribute: str, date_origin: datetime, value: float) -> dict:
        return {
            'source': source,
            'attribute': attribute,
            'timestamp': date_origin + random.random() * timedelta(days=1),
            'value': value
        }


if __name__ == '__main__':
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-c', '--config',
                            required=True,
                            dest='config_file',
                            help='Path to the input configuration file',
                            metavar='FILE')
    arg_parser.add_argument('-o', '--output',
                            required=True,
                            dest='validation_file',
                            help='Output file with stats from the input data, mey be used later to check if data '
                                 'processor calculate the statistical data correctly',
                            metavar='FILE')
    args = arg_parser.parse_args()
    configuration = ConfigurationParser.parse_from_file(args.config_file)
    DataProcessor().process_data(configuration, args.validation_file)
