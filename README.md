# Mon2Cas
Auto convert data from mongo to cassandra

## Feature:
1. Convert data from mongo to cassandra
2. Create schedule to sync the newest data or total data

## Configuration
* File: ./appconfig.js
```sh
module.exports = {
    mongo: {
        url: 'mongodb://localhost:27017/validium' //#Note: Url to connect to mongodb
    },
    table: { //#Note: Query schema in mongodb
        name: 'ExecutingLogs', //#Note: Url to connect to mongodb
        query: {
            event_type: 5, // RUN TESTCASE
            status: 1, // PASSED
            $or: [
                {synced: { $exists: false }},
                {synced: null}
            ]            
        },
        limit: 100 //# Limit records for each query
    },
    cassandra: { //#Note: Cassandra information to connect
        contactPoints: ['127.0.0.1'], 
        keyspace: 'validium_testresult',
        socketOptions: {
            connectTimeout: 60000,
            keepAlive: true,
            readTimeout: 60000
        } 
    }
}
```
## How to build
1. ``` cd mon2cas```
2. ```npm install```
3. Ensure content in ./schema.js and ./unique.js is ```module.exports = {}``` for the first running

## How to run
1. ```npm start``` (run daily, it only sync the newest data)
2. ```npm run all``` (scan and insert total data from mongo to cassandra. Need drop and recreate keyspace in cassandra before run it)