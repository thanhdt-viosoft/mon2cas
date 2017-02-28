DROP KEYSPACE validium_testresult;
CREATE KEYSPACE validium_testresult WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
