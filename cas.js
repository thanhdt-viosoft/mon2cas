const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: 'test' });
const uuid = require('node-uuid');
const _ = require('lodash');

exports = module.exports = {
    result: [],
    uuid(){
        return uuid.v4();
    },
    mapType(vl){
        if(typeof vl === 'function' && vl.name === 'TYPE') return `frozen<${vl.typeName}>`;
        if(vl instanceof Date) return 'timestamp';
        if(vl instanceof Array) {
            if((typeof vl[0] === 'function' && vl[0].name === 'TYPE') || vl instanceof TYPE) return `list<frozen<${vl[0].typeName}>>`;
            return `list<${exports.mapType(vl[0])}>`;
        }
        if(typeof vl === 'function' && vl.name === 'TYPE') return `list<frozen<${vl.typeName}>>`;
        if(typeof vl === 'string') return 'text';
        if(typeof vl === 'number') return 'int';
        return 'Unknown';
    },
    replaceToKey(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toLowerCase();
    },
    replaceToTable(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toUpperCase();
    },
    batch(queries){
        queries = queries.map((e) => {
            return {
                query: e,
                params: []
            };
        })
        return new Promise((resolve, reject) => {
            client.batch(queries, { prepare: true }, (err, result) => {
                if(err) return reject(err);
                resolve(result);
            }); 
        });
    },
    execute(query){
        return new Promise((resolve, reject) => {
            client.execute(query, [], (err, result) => {
                if(err) return reject(err);
                resolve(result);
            }); 
        });
    },
    createTable(tableName){
        tableName = tableName.toUpperCase();
        return new Promise((resolve, reject) => {
            const query = `CREATE TABLE IF NOT EXISTS ${tableName} (id varchar PRIMARY KEY);`;
            
            exports.result.push(query);
            client.execute(query, [], (err, result) => {
                if(err) return reject(err);
                resolve(tableName);
            }); 
        });        
    },
    convertToKeyLower(obj){
        for(var i in obj){
            const inew = exports.replaceToKey(i);
            if(obj[i] instanceof Array){
                for(var j in obj[i]){
                    obj[i][j] = exports.convertToKeyLower(obj[i][j]);                    
                }
                if( inew !== i){                
                    obj[exports.replaceToKey(i)] = obj[i];
                    delete obj[i];
                }                
            }else if(typeof obj[i] === 'object'){
                if( inew !== i){
                    obj[exports.replaceToKey(i)] = exports.convertToKeyLower(obj[i]);
                    delete obj[i];
                }
            }else {                
                if( inew !== i){
                    obj[inew] = _.clone(obj[i]);
                    delete obj[i];
                }                
            }
        }
        return obj;
    },
    renameAllArray(cols){
        for(let i in cols){
            const col = cols[i];
            if(col instanceof Array && typeof col[0] === 'object'){
                cols[i] = 'KEY NA';
            }else if(typeof col === 'object'){
                cols[i] = exports.renameAllArray(col);
            }
        }
        return cols;
    },
    insert(tableName, col){
        col = exports.convertToKeyLower(col);
        return new Promise((resolve, reject) => {
            const names = [];
            const values = [];
            const valuesObj = [];
            if(!col.id) col.id = exports.uuid();
            col = exports.renameAllArray(col);
            for(var k in col){                
                names.push(`${k.toLowerCase()}`);
                values.push('?');
                valuesObj.push(col[k]);
            }
            const query = `INSERT INTO ${tableName} (${names.join(', ')}) VALUES (${values.join(', ')});`;
            
            exports.result.push(query, JSON.stringify(valuesObj));
            client.execute(query, valuesObj, { prepare: true }, (err, result) => {
                if(err) return reject(err);
                resolve(result);
            }); 
        });        
    },
    addColumn(tableName, name, type){
        name = name.toLowerCase();
        return new Promise((resolve, reject) => {
            const query = `ALTER TABLE ${tableName.toUpperCase()} ADD ${name} ${exports.mapType(type)};`;
            
            exports.result.push(query);
            client.execute(query, [], (err, result) => {
                if(err) return reject(err);
                resolve(name);
            }); 
        });        
    },
    createType(name, col){
        name = name.toUpperCase();
        return new Promise((resolve, reject) => {
            const cols = [];
            for(var k in col){
                cols.push(`${k.toLowerCase()} ${exports.mapType(col[k])}`);
            }
            const query = `CREATE TYPE ${name} ( ${cols.join(',\n\t')} );`;
            
            exports.result.push(query);
            client.execute(query, [], (err, result) => {
                if(err) return reject(err);
                resolve(name);
            }); 
        });       
    },
    appendInColumn(tableName, col){
        tableName = tableName.toUpperCase();
        return new Promise((resolve, reject) => {
            const cols = [];
            for(var k in col){
                cols.push(`${k.toLowerCase()} ${exports.mapType(col[k])}`);
            }
            const query = `ALTER TABLE ${tableName} ADD (${cols.join(', ')});`;
            
            exports.result.push(query);
            client.execute(query, [], (err, result) => {
                if(err) return reject(err);
                resolve(tableName);
            }); 
        });           
    },
    appendInType(typeName, col){
        typeName = typeName.toUpperCase();
        return new Promise((resolve, reject) => {
            const cols = [];
            for(var k in col){
                cols.push(`${k.toLowerCase()} ${exports.mapType(col[k])}`);
            }
            const query = `ALTER TYPE ${typeName} ADD (${cols.join(', ')});`;
            
            exports.result.push(query);
            client.execute(query, [], (err, result) => {
                if(err) return reject(err);
                resolve(typeName);
            }); 
        });           
    }
};