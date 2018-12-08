# Big Data test environment

The application helps to test Big Data solutions

Requirements:  
*Python 3.7.1 64-bit*

Application can insert large number of random measurement data into MongoDB database.  
Example of measurement document inserted to the MongoDB:
```text
{
    "_id" : ObjectId("5c0bb6c580a5d94ed88d3a08"),
    "source" : "/data_sources/00001",
    "attribute" : "temperature",
    "timestamp" : ISODate("2018-01-01T08:26:13.863Z"),
    "value" : 5.0
}
```

How can I start?  
Run the Python <code>measurements-data-sender.py</code> script with specific configuration,  
add also output file with statistics that should be calculated by your Big Data analysis application.  
Example: to insert 1 million documents to MongoDb and generate output file stats.json:
```bash
python measurements-data-sender.py -c configs/config_1M.yaml -o stats.json
```
