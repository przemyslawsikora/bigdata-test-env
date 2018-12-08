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

    def process_data(self, config: ConfigurationModel):
        start_time = datetime.now()
        day_list = self.__prepare_days_table(config.time_line)
        measurement_count = config.sources * len(config.attributes) * \
                            len(day_list) * (config.measurements_in_block * 4 + 3)
        print('Number of all measurements to send: {:,}'.format(measurement_count))

        self.__prepare_mongo(config.mongo_config)
        for day in day_list:
            for s in range(1, config.sources + 1):
                source_name = '/data_sources/{:05}'.format(s)
                for attribute in config.attributes:
                    docs = []
                    for value in [config.values.q1, config.values.median, config.values.q3]:
                        docs.append(self.__build_measurement_doc(source_name, attribute, day, value))
                    self.input_collection.insert_many(docs)
                    for i in range(4):
                        self.__process_data_in_block(source_name, attribute, day, config)
        assert self.input_collection.count_documents({}) == measurement_count, \
            'Number of documents that should be sent ({}) is not equal to the number of ' \
            'documents in the Mongo collection ({})'.format(self.input_collection.count_documents({}),
                                                            self.input_collection.count_documents({}))
        self.client.close()
        elapsed = datetime.now() - start_time
        print("Program's execution taken {}".format(str(elapsed)))

    def __process_data_in_block(self, source: str, attribute: str, date_origin: datetime, config: ConfigurationModel):
        docs = []
        i = 0
        while i < config.measurements_in_block:
            value = random.uniform(config.values.minimum + 0.00001, config.values.maximum - 0.00001)
            docs.append(self.__build_measurement_doc(source, attribute, date_origin, value))
            i = i + 1
            if i % 128 == 0:
                self.input_collection.insert_many(docs)
                docs = []
        if docs:
            self.input_collection.insert_many(docs)

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
    args = arg_parser.parse_args()
    configuration = ConfigurationParser.parse_from_file(args.config_file)
    DataProcessor().process_data(configuration)
