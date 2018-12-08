# Big Data test environment

The application helps to test Big Data solutions

Requirements:  
*Python 3.7.1 64-bit*

Application can insert large amount of measurement data into MongoDB database.  
Example of measurement document inserted to the MongoDB:
```json
{
    "_id" : ObjectId("5c0bb6c580a5d94ed88d3a08"),
    "source" : "/data_sources/00001",
    "attribute" : "temperature",
    "timestamp" : ISODate("2018-01-01T08:26:13.863Z"),
    "value" : 5.0
}
```

Run the Python script with specific configuration, for example, to insert 1 million documents:
```bash
python bigdata-tester.py -c configs/config_1M.yaml
```
