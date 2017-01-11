const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const url = 'mongodb://localhost:27017/validium';
const cassandraService = require('./Cassandra.service');
const _ = require('lodash');
const cas = require('./cas');
const dateformat = require('date-format');
const limit = 100;
const isFullSync = !!process.argv[2];

let main = async (oldSessionId) => {
    const sessionId = !oldSessionId ? dateformat.asString("yyyyMMdd_hhmmss", new Date()) : oldSessionId;
    if(!oldSessionId) console.log(`STARTINT SYNC ${sessionId} WITH ${isFullSync ? "ALL" : "DAILY"} MODE`);
    let startTime = new Date();
    let convertObjectIdToString = (vl) => {
        if(vl._id) vl['id'] = _.clone(vl._id);
        delete vl._id;
        for(var i in vl){
            if(vl[i] instanceof ObjectID){
                vl[i] = vl[i].toString(); 
            }else if(typeof vl[i] === 'object'){
                vl[i] = convertObjectIdToString(vl[i]);
            }            
        }
        return vl;
    };
    MongoClient.connect(url, async function(err, db) {        
        const collection = db.collection('ExecutingLogs');
        if(isFullSync && !oldSessionId) {
            await collection.updateMany({}, {$set: {synced: false}});
        }
        const rs = await collection.find({
            event_type: 5, // RUN TESTCASE
            status: 1, // PASSED
            $or: [
                {synced: { $exists: false }},
                {synced: false}
            ]            
        }).limit(limit).toArray();
        await cas.open();
        let isReplay = rs.length === limit;
        let numPassed = 0;
        for(var i in rs){                          
            const item = convertObjectIdToString(_.cloneDeep(rs[i]));
            try {
                if(typeof item.result === 'string') item.result = JSON.parse(item.result);
                await cassandraService(sessionId, 'ExcutingLogs', item);
                console.log('.');
                try{
                    await collection.update({ _id: rs[i]._id }, { $set: { synced: true } });
                    numPassed++;
                }catch(e){
                    console.error('Update synced status to mongodb failed: ' + rs[i]._id);    
                }
            } catch (error) {
                console.error('Insert to cassandra failed: ' + rs[i]._id);
            }
        }
        await cas.close();
        await db.close();
        if(isReplay) {
            setTimeout(async function() {
                await main(sessionId);
            }, 500);            
        }
        else console.log(`Synced ${numPassed}/${rs.length} records in ${(new Date().getTime() - startTime)/1000} seconds`);
    });    
}

// let main1 = async () => {
//     await cassandraService('ExpensiveTbl', {
//         a: 1,
//         b: [
//             {c: {
//                 d: {
//                     e: 1
//                 }
//             }}
//         ],
//         d: [
//             {c: 'abc'}
//         ],
//     });
// }
main();