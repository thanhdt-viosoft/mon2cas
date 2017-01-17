# Mon2Cas
Auto convert data from mongo to cassandra

## Feature:
1. Convert data from mongo to cassandra
2. Create schedule to sync the newest data or total data

## How to build
1. ``` cd mon2cas```
2. ```npm install```
3. Ensure content in ./schema.js and ./unique.js is ```module.exports = {}``` for the first running

## How to run
1. ```npm start``` (run daily, it only sync the newest data)
2. ```npm run all``` (scan and insert total data from mongo to cassandra. Need drop and recreate keyspace in cassandra before run it)