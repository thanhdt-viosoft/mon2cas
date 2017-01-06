const _ = require('lodash');
const cas = require('./cas');

// let info = {
//     "Results": {
//         "Output": {
//             a: 1,
//             b: 'thanh'
//         },
//         "Infor": {
//             "Hosts": [{
//                 "Type": "SUT",
//                 "CPU": {
//                     "Processor": {
//                         "Model": "Intel(R) Xeon(R) CPU E5-2699 v3",
//                         "Clock": "2.30 GHZ",
//                         "Capabilities": "Fpu"
//                     },

//                     "Chipset": {
//                         "Model": "C612"
//                     },

//                     "NUMA": {
//                         "Count": "1"
//                     },

//                     "Core": {
//                         "CoreMapping": {
//                             "c0": "1",
//                             "c1": "5"
//                         }
//                     }
//                 },

//                 "Platforms": {
//                     "Server": {
//                         "Manufacturer": "Supermicro",
//                         "Product Name": "Proliant DL380 Gen9"
//                     },

//                     "Bios": {
//                         "HyperThreading": "enabled",
//                         "TurboBoost": "enabled",
//                         "Cstate": "enabled",
//                         "Pstate": "enabled",
//                         "EnhancedIntelSpeedstepTech": "disabled",
//                         "EnergyEfficientTurbo": "enabled",
//                         "IntelVTx": "disabled",
//                         "BiosVersion": "P89"
//                     }
//                 },

//                 "OS": {
//                     "Software": {
//                         "NetBridge": "Linux",
//                         "DPDK": "2.2.0",
//                         "PROX": "0.27"
//                     },

//                     "VMOS": {
//                         "Distribution": "Fedora",
//                         "Release": "Fedora 23",
//                         "Kernel": "4.2.3-300.fc23.x86_64"
//                     },

//                     "BootSetting": {
//                         "Hugepagesize": "1G",
//                         "NoOfHugepages": "56",
//                         "isolcpus": "1-27"
//                     },

//                     "Virtualization": {
//                         "Type": "Openstack",
//                         "Version": "WRS"
//                     }
//                 },

//                 "Peripherals": {
//                     "NIC": {
//                         "Manufacturer": "Intel Corporation",
//                         "Product": "Niantic",
//                         "Capacity": "1G",
//                         "Socket": "0"
//                     },

//                     "Memory": {
//                         "Manufacturer": "Micron",
//                         "Type": "DDR4",
//                         "Capacity": "8G x n channel, total: 273787 G ",
//                         "Configured Clock": "1866 MHZ"
//                     },

//                     "Storage": {
//                         "Manufacturer": "SanDisk",
//                         "Capacity": "1000 GB"
//                     },

//                     "PCIe": {
//                         "Slots": "8",
//                         "Version": "3"
//                     }
//                 }
//             }, {
//                 "Type": "Tester",
//                 "CPU": {
//                     "Processor": {
//                         "Model": "Intel(R) Xeon(R) CPU E5-2699 v3",
//                         "Clock": "2.50 GHZ",
//                         "Capabilities": "mmx, sse"
//                     },

//                     "Chipset": {
//                         "Model": "C612"
//                     },

//                     "NUMA": {
//                         "Count": "1"
//                     },

//                     "Core": {
//                         "CoreMapping": {
//                             "c0": "2",
//                             "c1": "6"
//                         }
//                     }
//                 },

//                 "Platforms": {
//                     "Server": {
//                         "Manufacturer": "DELL",
//                         "Product Name": "Proliant DL380 Gen9"
//                     },

//                     "Bios": {
//                         "HyperThreading": "enabled",
//                         "TurboBoost": "disabled",
//                         "Cstate": "enabled",
//                         "Pstate": "disabled",
//                         "EnhancedIntelSpeedstepTech": "enabled",
//                         "EnergyEfficientTurbo": "enabled",
//                         "IntelVTx": "enabled",
//                         "BiosVersion": "P89"
//                     }
//                 },

//                 "OS": {
//                     "Software": {
//                         "NetBridge": "Linux",
//                         "DPDK": "2.2.0",
//                         "PROX": "0.27"
//                     },

//                     "VMOS": {
//                         "Distribution": "Ubuntu",
//                         "Release": "Ubuntu 14",
//                         "Kernel": "4.2.3-300.fc23.x86_64"
//                     },

//                     "BootSetting": {
//                         "Hugepagesize": "1G",
//                         "NoOfHugepages": "56",
//                         "isolcpus": "1-27"
//                     },

//                     "Virtualization": {
//                         "Type": "QEMU-KVM",
//                         "Version": "Kilo"
//                     }
//                 },

//                 "Peripherals": {
//                     "NIC": {
//                         "Manufacturer": "Intel Corporation",
//                         "Product": "Fortville",
//                         "Capacity": "10G",
//                         "Socket": "1"
//                     },

//                     "Memory": {
//                         "Manufacturer": "Samsung",
//                         "Type": "DDR4",
//                         "Capacity": "8G x n channel, total: 273787 G ",
//                         "Configured Clock": "1866 MHZ"
//                     },

//                     "Storage": {
//                         "Manufacturer": "Seagate",
//                         "Capacity": "1000 GB"
//                     },

//                     "PCIe": {
//                         "Slots": "8",
//                         "Version": "3"
//                     }
//                 }

//             }]
//         }
//     }
// };

let info = {
    Infor: {
        c: [
            {e: 1}
        ]
    }
}

global.TYPE = (typeName) => {
    const TYPE = class TYPE {};
    TYPE.typeName = typeName.toUpperCase();
    return TYPE;
}
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
    constructor(name, value, column) {
        super(name);
        this.name = Type.toType(name);
        this.column = column;
        this.items = {};

        for(let i in value){
            const ftype = Type.toType(i);
            const item = value[i];
            if(_.isNil(item)) continue;
            if(item instanceof Array){
                if(item.length === 0) continue;
                if(item[0] instanceof Array){
                    throw 'array in array';
                }else if(typeof item[0] === 'object'){
                    let allItem = {};
                    for(let i of item){
                        allItem = _.merge(allItem, i);
                    }
                    for(let i in item){
                        item[i][ftype] = this.column.table.id;
                    }
                    allItem[ftype] = "#FOREIGN KEY";
                    this.items[ftype] = new Table(`${this.name}_${ftype}`, allItem, _.clone(item));
                }else {
                    this.items[ftype] = [item[0]];
                }
                
            }else if(typeof item === 'object'){
                this.items[ftype] = new Type(`TYPE__${this.name}_${ftype}`, item, this.column);
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
        await cas.createType(this.name, items0);
        return TYPE(this.name);
    }

    async insert(){
        for(let i in this.items){
            const item = this.items[i];
            if(item instanceof Table){ 
                return await item.insert();    
            }
        }
    }
    
    static toType(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toUpperCase();
    }
}
global.Column = class Column extends AbsCasObj {
    constructor(name, value, table){
        super(name);
        this.name = Column.toColumn(name);
        this.table = table;
        if(value instanceof Array){
            if(value.length > 0) {
                if(value[0] instanceof Array){
                    throw 'array in array';
                }else if(typeof value[0] === 'object'){
                    let allValues = {};
                    for(let i of value){
                        allValues = _.merge(allValues, i);
                    }
                    for(let i in value){
                        value[i][this.name] = this.table.id;
                    }
                    allValues[this.name] = '#FOREIGN KEY';          
                    this.type = new Table(`${this.table.name}_${this.name}`, allValues, _.clone(value));
                }else {
                    this.type = [value[0]];
                }
            }
        }else if(typeof value === 'object'){
            this.type = new Type(`TYPE__${this.table.name}_${this.name}`, value, this);            
        }else {
            this.type = value;
        }
    }

    async create(){
        let type0 = _.clone(this.type);
        if(this.type instanceof Type) this.type = await this.type.create();
        if(this.type instanceof Table) {
            type0 = await this.type.create();
        }
        return type0;
    }

    async insert(){
        return await this.type.insert(); 
    }

    static toColumn(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toLowerCase();
    }
}
global.Table = class Table extends AbsCasObj {
    constructor(name, cols, values) {
        super(name);
        this.name = Table.toTable(name);
        this.values = [];
        if(!(values instanceof Array)) this.values = [values];
        else this.values = values;
        this.id = cas.uuid();

        this.column = {};
        for(let c in cols){
            let fname = Column.toColumn(c);
            let item = cols[c];
            if(_.isNil(item)) continue;
            if(item instanceof Array){
                if(item.length === 0) continue;
                if(item[0] instanceof Array){
                    throw 'array in array';
                }else if(typeof item[0] === 'object'){
                    let allItem = {};
                    for(let ii in item){
                        allItem = _.merge(allItem, item[ii]);                        
                    }
                    for(let i in item){
                        item[i][fname] = this.id;
                    }
                    this.column[fname] = new Table(this.name, allItem, _.clone(item));                    
                }else {                    
                    this.column[fname] = [item[0]];
                }
            }else if(typeof item === 'object'){
                this.column[fname] = new Column(c, item, this);
            }else {
                this.column[fname] = item;
            }
        }
    }

    async insert(){        
        for(var i in this.values){
            try {
                const item = this.values[i];
                await cas.insert(this.name, this.values[i]);
            } catch (error) {
                console.log(error);
            }
            
        }          
    }
    
    async create(){     
        // TODO: CHeck o day type co van de, ko append duoc , type unknown
        let column0 = _.clone(this.column);   
        for(let i in this.column){
            const item = this.column[i];
            if(item instanceof Column){
                column0[i] = await item.create();
            }else if(item instanceof Table){
                column0[i] = await item.create();
            }
        }
        await cas.createTable(this.name);        
        await cas.appendInColumn(this.name, column0);
        return this.id;
    }

    async insertFirst(){
        for(let i in this.column){
            const item = this.column[i];
            if(item instanceof Table){
                await item.insert();  
            }
        }
    }
    
    static toTable(key){
        return key.replace(/[^\w_]/g, '_').replace(/_+/g, '_').toUpperCase();
    }
}
const main = async(tbl, cols) => {
    let table = new Table(tbl, cols, cols);
    await table.create();    
    await table.insertFirst();
    require('fs').writeFileSync('./test.txt', cas.result.join('\n'), 'UTF-8');    
    //  try {
    //     for(var q of cas.result){
    //         const rs = await cas.execute(q);    
    //     }
    // } catch (error) {
    //     console.log(error);
    // }   
    console.log('done');
}
// let addPath = (parent, type) => {
//     return parent.length > 0 ? parent+'.'+type : type;
// }
// let handleTable = async (key, values) => {
//     for(let field in values){
//         let table = new Table(field);
//         await handleType(table, field, values);
//     }
// }
// let handleType = async (table, key, obj, level=0, pschema='') => { 
//     const typeName = cas.replaceToKey(key);   
//     const ownSchema = addPath(pschema, cas.replaceToKey(key));
//     let typeValue;
//     if(obj instanceof Array) {
//         for(let i in obj){
//             const [schema, objType, typeValue] = handleType(obj[i]);            
//             if(i === 0){
//                 typeValue = [typeValue];
//             }
//         }        
//     } else if(typeof obj === 'object'){
//         typeValue = TYPE(`TYPE_${table.name}__${typeName}_${level}`.toUpperCase());
//         let col = table.addColumn(key, typeValue);
//         let objMeta = {};
//         for(let i in obj){
//             const colName = cas.replaceToKey(i);
//             const schemaPath = addPath(ownSchema, colName);
//             const [schema, objType, typeValue] = handleType(table, i, obj[i], level+1, schemaPath);
//             objMeta[colName] = typeValue;
//             col.add
//         }                        
//     } else {
//         typeValue = obj;
//         table.addColumn(key, typeValue);
//     }        
//     return Promise.resolve([ownSchema, objType, typeValue]);
// }

// const main = async(colName, value) => {
//     const tblName = `TEST01`;
//     let schema = {};
//     let oldSchema = {};
//     try {
//         // oldSchema= require('./schema.js');  
//     } catch (error) {
//         oldSchema = {};
//     } 
//     const addPath = (parent, type) => {
//         return parent.length > 0 ? parent+'.'+type : type;
//     }
//     const handleTable = async(key, obj)=>{ 
//         const types = await handleObject(key, obj[0], 0, '');   
//         cas.createTable(key, types.objType);
//     };
//     const handleObject = async(key, obj, level, parent)=>{ 
//         let objType = {};
//         for(var k in obj){
//             const kType = cas.replaceToKey(k);
//             const kPath = addPath(parent, kType);
//             if(obj[k] instanceof Array){
//                 if(typeof obj[k][0] === 'object'){
//                     await handleTable(k, obj[k]);
//                     objType[kType] = TYPE(`varchar`);
//                     // // create new table                    
//                     // const types = await handleObject(k, obj[k][0], level+1, kPath);   
//                     // cas.createTable(kType, types.objType);
//                     // if(!oldSchema[kPath]) objType[kType] = [TYPE(`TYPE___${tblName}__${kType}_${level+1}`)];
//                 }else{
//                     if(!oldSchema[kPath]) objType[kType] = [obj[k][0]];
//                 }                
//             }else if(typeof obj[k] === 'object'){                
//                 await handleObject(k, obj[k], level+1, kPath);   
//                 if(!oldSchema[kPath]) objType[kType] = TYPE(`TYPE___${tblName}__${kType}_${level+1}`);
//             }else {
//                 if(!oldSchema[kPath]) objType[kType] = obj[k];
//             }
//             if(objType[kType]) schema[kPath] = cas.mapType(objType[kType]);
//         }
//         const typeName = `TYPE___${tblName}__${cas.replaceToKey(key).toUpperCase()}_${level}`;
//         if(Object.keys(objType).length > 0){
//             let isExisted = !Object.keys(oldSchema).length === 0 ? null : _.values(oldSchema).find((e) => {
//                 return e.indexOf(typeName) !== -1;
//             });
//             if(!isExisted) {
//                 await cas.createType(typeName, objType);
//             }else {
//                 await cas.appendInType(typeName, objType);
//             }            
//         }        
//         return Promise.resolve({schema, objType});
//     }
//     try{        
//         await cas.createTable(tblName);        
//         schema = await handleObject(colName, value, 0, '');
//         await cas.addColumn(tblName, colName, TYPE(`TYPE___${tblName}__${cas.replaceToKey(colName)}_${0}`));                
//     }catch(e){
//         console.log(e);
//     }
//     require('fs').writeFileSync('./schema.js', 'module.exports=' + JSON.stringify(_.merge(oldSchema, schema), null, '\t'), 'UTF-8');
//     require('fs').writeFileSync('./test.txt', cas.result.join(';\n'), 'UTF-8');
//     // try {
//     //     for(var q of cas.result){
//     //         const rs = await cas.execute(q);    
//     //     }
//     //     await cas.insert(tblName, colName, value);
//     // } catch (error) {
//     //     console.log(error);
//     // }        
// };

main("Result", info);
