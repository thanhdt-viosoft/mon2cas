const cassandra = require('cassandra-driver');
const uuid = require('node-uuid');
const _ = require('lodash');
const fs = require('fs');

let client;
exports = module.exports = {
    result: {
        schema: [],
        insert: []
    },
    debug: true,
    _unique: require('./unique'),
    unique(name0, unique){
        let name = name0.replace(/^[0-9]+/g, '').replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_/g, '').replace(/_$/g, '');
        if(!exports._unique[unique]){
            exports._unique[unique] = name + (Object.keys(exports._unique).length > 0 ? ('_' + Object.keys(exports._unique).length) : '');
        }
        return exports._unique[unique];
    },
    saveUnique(){
        fs.writeFileSync('./unique.js', 'module.exports = ' + JSON.stringify(exports._unique, null, '\t'));
    },
    uuid(){
        return uuid.v4();
    },
    reset(){
        exports.result.schema = [];
        exports.result.insert = [];
    },
    mapType(vl){
        if(vl instanceof Date) return 'timestamp';
        if(vl instanceof Array){
            if(vl[0] instanceof Array){
                throw 'Not support array in array';
            }
            return `list<${exports.mapType(vl[0])}>`;
        }
        if(typeof vl === 'function' && vl.name === 'TYPE') return `frozen<${vl.typeName}>`;
        if(typeof vl === 'string') return 'varchar';
        if(typeof vl === 'number') return 'int';
        if(typeof vl === 'boolean') return 'boolean';
        return 'Unknown';
    },
    replaceToKey(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toLowerCase();
    },
    replaceToTable(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toUpperCase();
    },
    renameAllArray(cols, key, unique='$root'){
        for(let i in cols){
            const col = cols[i];
            if(col instanceof Array && typeof col[0] === 'object'){
                cols[i] = `#MAP(${exports.unique(key, `${unique}.${i}`)}.${i})`;
            }else if(typeof col === 'object'){
                cols[i] = exports.renameAllArray(col, i, `${unique}.${i}`);
            }
        }
        return cols;
    },
    execute: (q, prms=[]) => {
        return new Promise((resolve, reject) => {
            client.execute(q, prms, {prepare: true}, (err, rs) => {
                if(err) return reject(err);
                resolve(rs);
            });
        });
    },
    queries(queries){
        return new Promise(async (resolve, reject) => {                                    
            try{
                for(let q of queries){
                    console.log(`==> ${q}`);
                    await exports.execute(q);
                }   
                resolve('Done');
            }catch(e){                        
                reject(e);
            }
        });        
    },
    batch(queries){
        queries = queries.map((e) => {
            if(typeof e === 'string'){
                return {
                    query: e,
                    params: []
                };
            }else {
                return {
                    query: e.query,
                    params: e.prms
                };
            }
        })
        return new Promise((resolve, reject) => {
            client.batch(queries, { prepare: true }, (err, result) => {
                resolve(result);                   
            });
        });
    },
    createTable(tableName){
        // tableName = tableName.toUpperCase();
        const query = `CREATE TABLE ${tableName} (id varchar PRIMARY KEY);`;            
        exports.result.schema.push(query);
        return tableName;
    },
    insert(tableName, col){
        const names = [];
        const values = [];
        const valuesObj = [];
        if(!col.id) col.id = exports.uuid();
        col = exports.renameAllArray(col, tableName);
        for(var k in col){                
            names.push(`${k.toLowerCase()}`);
            values.push('?');
            valuesObj.push(col[k]);
        }
        const query = `INSERT INTO ${tableName} (${names.join(', ')}) VALUES (${values.join(', ')});`;
        exports.result.insert.push({query: query, prms: valuesObj});
        return col.id;    
    },
    addColumn(tableName, name, type){
        // name = name.toLowerCase();
        if(name !== 'id') {
            const query = `ALTER TABLE ${tableName.toUpperCase()} ADD ${name} ${exports.mapType(type)};`;            
            exports.result.schema.push(query);
        }
        return name;
    },
    createType(name, col){
        // name = name.toUpperCase();
        const cols = [];
        for(var k in col){
            cols.push(`${k.toLowerCase()} ${exports.mapType(col[k])}`);
        }
        const query = `CREATE TYPE ${name} ( \n\t${cols.join(',\n\t')} \n);`;        
        exports.result.schema.push(query);
        return name;
    },
    appendInColumn(tableName, col){
        // tableName = tableName.toUpperCase();
        const cols = [];
        for(var k in col){
            if(k === 'id') continue;
            cols.push(`${k.toLowerCase()} ${exports.mapType(col[k])}`);
        }
        const query = `ALTER TABLE ${tableName} ADD (\n\t${cols.join(',\n\t')}\n);`;        
        exports.result.schema.push(query);
        return tableName;
    },
    appendInType(typeName, col){
        // typeName = typeName.toUpperCase();
        for(var k in col){
            const query = `ALTER TYPE ${typeName} ADD ${k.toLowerCase()} ${exports.mapType(col[k])};`;
            exports.result.schema.push(query);
        }
        return typeName;
    }, open(){
        return new Promise((resolve, reject) => {
            client = new cassandra.Client(appConfig.cassandra);
            client.connect(async (err) => {
                if(err) return reject(err);
                resolve('Done');
            });
        });
    }, close(){
        return new Promise((resolve, reject) => {
           client.shutdown((err) => {
                if(err) return reject('ERROR SHUT DOWN');
                resolve('Done');
            }); 
        });        
    }
};