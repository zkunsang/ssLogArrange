const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');

class MongoConnectionHelper {
    constructor() {}

    async setConnection(dbMongo) {
        const url = dbMongo.host;
        const sslCrt = dbMongo.sslCrt;
        const options = { useUnifiedTopology: true, ignoreUndefined: true };

        if (sslCrt) {
            const ca = [fs.readFileSync(sslCrt)];
            options.useFindAndModify = false;
            options.retryWrites = false;
            options.sslValidate = true;
            options.sslCA = ca;
            options.useNewUrlParser = true;
            options.useUnifiedTopology = true;
        }
        
        return await MongoClient.connect(url, options);
    }
}


module.exports = new MongoConnectionHelper();
