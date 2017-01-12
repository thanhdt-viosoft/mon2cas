const _ = require('lodash');
const cas = require('./cas');
const fs = require('fs');

global.TYPE = (typeName) => {
    const TYPE = class TYPE {};
    TYPE.typeName = typeName.toUpperCase();
    return TYPE;
}
let oldSchema = require('./schema');
class AbsCasObj {
    constructor(name) {
        this.name = name;
    }

    toResult(){
        
    }

    mapType(vl){
        if(typeof vl === 'function' && vl.name === 'TYPE') return `frozen<${vl.typeName}>`;
        if(vl instanceof Date) return 'timestamp';
        if(vl instanceof Array) {
            if(typeof vl[0] === 'function' && vl[0].name === 'TYPE') return `list<frozen<${vl[0].typeName}>>`;
            return `list<${exports.mapType(vl[0])}>`;
        }
        if(typeof vl === 'string') return 'text';
        if(typeof vl === 'number') return 'int';
        return 'Unknown';
    }    
}
global.Type = class Type extends AbsCasObj {
    constructor(name, value, column, unique) {
        super(name);        
        this.name = Type.toType(name, unique);
        this.column = column;
        this.items = {};

        for(let ftype in value){
            const item = value[ftype];
            if(_.isNil(item)) continue;
            if(item instanceof Array){
                if(item.length === 0) continue;
                if(item[0] instanceof Array){
                    throw 'array in array';
                }else if(item[0] instanceof Date){
                    this.items[ftype] = item[0];
                }else if(typeof item[0] === 'object'){
                    let allItem = {};
                    for(let i of item){
                        allItem = _.merge(allItem, i);
                    }
                    for(let i in item){
                        item[i][ftype] = "#REPLACE_TO(TABLE_ID)";
                    }
                    allItem[ftype] = `#FOREIGN KEY`;
                    this.items[ftype] = new Table(ftype, allItem, _.clone(item), unique+'.'+ftype);
                }else {
                    this.items[ftype] = [item[0]];
                }
                
            }else if(item instanceof Date){
                this.items[ftype] = item;
            }else if(typeof item === 'object'){
                this.items[ftype] = new Type(ftype, item, this.column, unique+'.'+ftype);
            }else {
                this.items[ftype] = item;
            }            
        }    
    }

    async create(){        
        let items0 = _.clone(this.items);
        for(let i in this.items){
            const item = this.items[i];
            if(item instanceof Type){                
                items0[i] = await item.create();
            }else if(item instanceof Table){       
                items0[i] = await item.create();              
            }
        }
        let typeAppend = {};
        for(var i in items0){
            if(oldSchema[`${this.name}.${i}`] !== "COLUMN_IN_TYPE"){
                typeAppend[i] = items0[i];
            }
        }
        if(Object.keys(typeAppend).length > 0){
            if(oldSchema[`${this.name}`] !== 'TYPE'){            
                cas.createType(this.name, typeAppend);
                oldSchema[`${this.name}`] = "TYPE";
            }else {
                cas.appendInType(this.name, typeAppend);
            }
            for(var i in typeAppend){
                oldSchema[`${this.name}.${i}`] = "COLUMN_IN_TYPE";
            }
        }        
        return TYPE(this.name);
    }

    async insert(id){
        for(let i in this.items){
            const item = this.items[i];
            if(item instanceof Table){ 
                await item.insert(id);    
            }else if(item instanceof Type){
                await item.insert(id);    
            }
        }
    }
    
    static toType(key, unique){
        return cas.unique(key.toUpperCase(), unique);
    }
}
global.Column = class Column extends AbsCasObj {
    constructor(name, value, table, unique){
        super(name);
        this.name = Column.toColumn(name);
        this.table = table;
        if(value instanceof Array){
            if(value.length > 0) {
                if(value[0] instanceof Array){
                    throw 'array in array';
                }else if(value[0] instanceof Date){
                    this.type = value[0];
                }else if(typeof value[0] === 'object'){
                    let allValues = {};
                    for(let i of value){
                        allValues = _.merge(allValues, i);
                    }
                    for(let i in value){
                        value[i][this.name] = "#REPLACE_TO(TABLE_ID)";
                    }
                    allValues[this.name] = '#FOREIGN KEY';          
                    this.type = new Table(name, allValues, _.clone(value), unique);
                }else {
                    this.type = [value[0]];
                }
            }
        }else if(value instanceof Date){
            this.type = value;
        }else if(typeof value === 'object'){
            this.type = new Type(name, value, this, unique);            
        }else {
            this.type = value;
        }
    }

    async create(){
        if(this.type instanceof Type) {            
            let vl = await this.type.create();
            return vl;
        }
        if(this.type instanceof Table) {
            let vl = await this.type.create();
            return vl;
        }            
        return this.type;
    }

    async insert(id){
        if(this.type instanceof Type) {            
            await this.type.insert(id); 
        }
        if(this.type instanceof Table) {
            await this.type.insert(id); 
        }            
    }

    static toColumn(key){
        return key;
    }
}
global.Table = class Table extends AbsCasObj {
    constructor(name, cols, values, unique='$root') {        
        super(name);
        this.name = Table.toTable(name, unique);
        this.values = [];
        if(!(values instanceof Array)) this.values = [values];
        else this.values = values;

        this.column = {};
        for(let fname in cols){
            let item = cols[fname];
            if(_.isNil(item)) continue;
            if(item instanceof Array){
                if(item.length === 0) continue;
                if(item[0] instanceof Array){
                    throw 'array in array';
                }else if(item[0] instanceof Date){
                    this.column[fname] = item[0];
                }else if(typeof item[0] === 'object'){
                    let allItem = {};
                    for(let ii in item){
                        allItem = _.merge(allItem, item[ii]);                        
                    }
                    for(let i in item){
                        item[i][fname] = "#REPLACE_TO(TABLE_ID)";
                    }
                    allItem[fname] = '#FOREIGN KEY';       
                    this.column[fname] = new Table(fname, allItem, _.clone(item), unique+"."+fname);                    
                }else {                    
                    this.column[fname] = [item[0]];
                }
            }else if(item instanceof Date){
                this.column[fname] = item;
            }else if(typeof item === 'object'){
                this.column[fname] = new Column(fname, item, this, unique+"."+fname);
            }else {
                this.column[fname] = item;
            }
        }
    }

    async insert(id){       
        for(var i in this.values){
            if(id){
                for(var k in this.values[i]){
                    if(this.values[i][k] === "#REPLACE_TO(TABLE_ID)"){
                       this.values[i][k] = id;
                    }
                }
            }
            const item = this.values[i];
            this.id = cas.insert(this.name, this.values[i]);
            for(var key in this.column){
                const col = this.column[key];
                if(col instanceof Table){
                    const rs = col.insert(this.id);
                }else if(col instanceof Type){
                    const rs = col.insert(this.id);
                }else if(col instanceof Column) {
                    const rs = col.insert(this.id);
                }
            }
        }          
    }
    
    async create(){     
        // TODO: CHeck o day type co van de, ko append duoc , type unknown
        let column0 = _.cloneDeep(this.column);   
        for(let i in this.column){
            const item = this.column[i];            
            if(item instanceof Column){
                column0[i] = await item.create();
            }else if(item instanceof Table){
                column0[i] = await item.create();
            }
        }
        if(oldSchema[this.name] !== 'TABLE'){
            cas.createTable(this.name);        
            oldSchema[this.name] = 'TABLE';
        }
        let columnAppend = {};
        for(var i in column0){
            if(oldSchema[`${this.name}.${i}`] !== "COLUMN"){
                columnAppend[i] = column0[i];
            }
        }
        if(Object.keys(columnAppend).length > 0){
            cas.appendInColumn(this.name, columnAppend);
            for(var i in columnAppend){
                oldSchema[`${this.name}.${i}`] = "COLUMN";
            }
        }
        return 'ID TABLE';
    }
    
    static toTable(key, unique){
        return cas.unique(key.toUpperCase(), unique);//.replace(/^TYPE__/, '');
    }
}
let reformatObject = (obj) => {
    let toKey = (key) => {
        if(!isNaN(parseInt(key))) return 'unknown_'+key;
        key = key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toLowerCase();
        if(['add','all','allow','alter','and','any','apply','as','asc','ascii','authorize','batch','begin','bigint','blob','boolean','by','clustering','columnfamily','compact','consistency','count','counter','create','custom','decimal','delete','desc','distinct','double','drop','each_quorum','exists','filtering','float','from','frozen','full','grant','if','in','index','inet','infinity','insert','int','into','key','keyspace','keyspaces','level','limit','list','local_one','local_quorum','map','modify','nan','norecursive','nosuperuser','not','of','on','one','order','password','permission','permissions','primary','quorum','rename','revoke','schema','select','set','static','storage','superuser','table','text','timestamp','timeuuid','three','to','token','truncate','ttl','tuple','two','type','unlogged','update','use','user','users','using','uuid','values','varchar','varint','where','with','writetime'].includes(key)) key += '_';        
        return key;
    };
    let rs = {};
    for(var i in obj){        
        let vi = toKey(i);
        if(obj[i] instanceof Array){
            if(obj[i].length > 0) {
                rs[vi] = [];
                for(let j in obj[i]){
                    if(obj[i][j] instanceof Array) throw 'not support array in array';
                    if(_.isPlainObject(obj[i][j])) {      
                        rs[vi].push(reformatObject(obj[i][j]));
                    }else {
                        rs[vi].push(obj[i][j]);
                    }
                }
            }else {
                // console.log('remove');
            }
        }else if(_.isPlainObject(obj[i])) {  
            let tmp = reformatObject(obj[i]);
            if(Object.keys(tmp).length > 0){          
                rs[vi] = tmp;
            }else {
                // console.log('remove');
            }
        }else {
            if(!_.isNil(obj[i])) rs[vi] = obj[i];
        }
    }
    return rs;
}
const main = async(sessionId, tbl, cols) => {
    cols = reformatObject(cols);    
    cas.reset();
    let cols1 = _.cloneDeep(cols);
    let table = new Table(tbl, cols1, cols1);
    await table.create();
    if(cas.result.schema.length > 0){
        let cnt = [];
        cnt.push('\n---------S=C=H=E=M=A---------\n');
        for(var s of cas.result.schema){
            cnt.push(s);
        }
        fs.appendFileSync(`./logs/${sessionId}.running.sql`, cnt.join('\n'), 'UTF-8');
        cnt = null;
    }    
    cas.saveUnique();
    await table.insert();
    if(cas.result.insert.length > 0){
        let cnt = [];
        cnt.push('\n---------D=A=T=A---------\n');
        for(var s of cas.result.insert){
            cnt.push(s.query);
            cnt.push(JSON.stringify(s.prms, null, '\t'));
        }
        fs.appendFileSync(`./logs/${sessionId}.running.sql`, cnt.join('\n'), 'UTF-8');
        cnt = null;
    }    
    if(cas.result.schema.length > 0) {
        console.log('\n======S=C=H=E=M=A======\n');
        await cas.queries(cas.result.schema);
        fs.writeFileSync('./schema.js', 'module.exports = ' + JSON.stringify(oldSchema, null, '\t'), 'UTF-8');
    }
    if(cas.result.insert.length > 0) {
        await cas.batch(cas.result.insert);        
    }  
}
module.exports = main;