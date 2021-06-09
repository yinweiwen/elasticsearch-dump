const base = require("./base");
const { MongoClient } = require("mongodb");
const _ = require('lodash')

class mongo extends base {
    constructor(parent, file, options) {
        super(parent, file, options)
        const HOSTS_RX = /(mongodb(?:\+srv|)):\/\/(?: (?:[^:]*) (?: : ([^@]*) )? @ )?([^/?]*)(?:\/|)(.*)/;
        if (!file.match(HOSTS_RX)) {
            throw new Error("Invalid mongo connection string");
        }
        this.client = new MongoClient(file, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        this.client.connect()
            .then(() => {
                this.db = this.client.db(this.parent.options.mongoDb)
            })
        this._reading = false
    }

    async setupGet(offset) {
        console.log("set up");
    }

    __handleData(data) {
        let lineCounter = 0
        let arr = []
        data.forEach(elem => {
            let targetElem = elem
            if (elem._source) {
                targetElem = _.defaults(
                    {
                        ...elem._source
                    })

                if (this.parent.options.mongoId) {
                    targetElem._id = elem._id
                }
            }
            if(elem._source.collect_time){
                targetElem.collect_time=new Date(elem._source.collect_time)
            }
            if(elem._source.create_time){
                targetElem.create_time=new Date(elem._source.create_time)
            }
            if (!this.collection) {
                this.collection = this.db.collection(elem._index)
            }
            arr.push(targetElem)
            // this.log(targetElem)
            lineCounter++
        })
        this.log(arr)
        return lineCounter
    }

    // accept arr, callback where arr is an array of objects
    // return (error, writes)
    set(data, limit, offset, callback) {
        const error = null
        let lineCounter = 0

        if (data.length === 0) {
            if (!!this.client) {
                this.client.close()
                    .then(() => {
                        return callback(null, lineCounter)
                    });
            }
        } else {
            lineCounter += this.__handleData(data)
            process.nextTick(() => callback(error, lineCounter))
        }
    }

    log(line) {
        // this.collection.insertOne(line)
        this.collection.insertMany(line)
    }
}



module.exports = {
    mongo
}