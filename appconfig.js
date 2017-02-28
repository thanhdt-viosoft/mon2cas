module.exports = {
    mongo: {
        url: 'mongodb://localhost:27017/validium'
    },
    table: {
        name: 'ExecutingLogs',
        query: {
            event_type: {
                $in: [5, 6]
            }, // RUN TESTCASE
            status: 1, // PASSED
            $or: [
                {synced: { $exists: false }},
                {synced: null}
            ]            
        },
        limit: 100
    },
    cassandra: {
        contactPoints: ['127.0.0.1'], 
        keyspace: 'validium_testresult',
        socketOptions: {
            connectTimeout: 60000,
            keepAlive: true,
            readTimeout: 60000
        } 
    }
}