const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const _ = require('lodash');
const dateformat = require('date-format');
const fs = require('fs');

const cassandraService = require('./Cassandra.service');
const cas = require('./cas');
const isFullSync = !!process.argv[2];

global.appConfig = require('./appconfig');

let main = async (oldSessionId) => {
    const sessionId = !oldSessionId ? dateformat.asString("yyyyMMdd_hhmmss", new Date()) : oldSessionId;
    if(!oldSessionId) console.log(`STARTING SYNC ${sessionId} WITH ${isFullSync ? "ALL" : "DAILY"} MODE`);
    let startTime = new Date();
    let convertObjectIdToString = (vl) => {
        if(vl && typeof vl === 'object' && vl._id) {
            vl['id'] = _.clone(vl._id);
            delete vl._id;
        }
        for(var i in vl){
            if(vl[i] instanceof ObjectID){
                vl[i] = vl[i].toString(); 
            }else if(typeof vl[i] === 'object'){
                vl[i] = convertObjectIdToString(vl[i]);
            }            
        }
        return vl;
    };
    MongoClient.connect(appConfig.mongo.url, async function(err, db) {        
        const collection = db.collection(appConfig.table.name);
        if(isFullSync && !oldSessionId) {
            await collection.updateMany({}, {$set: {synced: null}});
        }
        let rs = await collection.find(appConfig.table.query).limit(appConfig.table.limit).toArray();
        await cas.open();
        let isReplay = rs.length === appConfig.table.limit;
        let numPassed = 0;
        for(var i in rs){                          
            const item = convertObjectIdToString(_.cloneDeep(rs[i]));
            try {
                if(typeof item.result === 'string') item.result = JSON.parse(item.result);
                await cassandraService(sessionId, appConfig.table.name, item.result);
                console.log('.');
                try{
                    await collection.update({ _id: rs[i]._id }, { $set: { synced: sessionId } });
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
        else {            
            if(rs.length > 0) {
                console.log(`SYNCED ${numPassed}/${rs.length} RECORDS IN ${(new Date().getTime() - startTime)/1000}s`);
                fs.renameSync(`./logs/${sessionId}.running.sql`, `./logs/${sessionId}.${numPassed === rs.length ? 'passed' : 'failed'}.${numPassed}.${rs.length}.sql`);
            }else {
                console.log('THERE ARE NO NEW RECORDS');
            }
        }
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