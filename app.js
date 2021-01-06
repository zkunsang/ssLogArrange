const MongoConnectionHelper = require('./MongoConnectionHelper');
const configs = require('./configs');
const events = require('events');
const DateUtil = require('./utils/DateUtil');
const eventEmitter = new events.EventEmitter();
const moment = require('moment');

let sourceDbConnections = {};
let targetDb = null;
let sourceDb = null;

const COLLECTION_LIST = {
    INVEN: 'inven',
    LOGIN: 'login',
    NETWORK: 'network',
    STORY_LOG: 'story_log',
    PRODUCT: 'product'
}

const SOURCE_DB = 'log';
const TARGET_DB = 'log';

eventEmitter.on('processing', async () => {
    console.log("process start");
    const invenDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.INVEN]
    );

    const loginDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.LOGIN]
    );

    const networkDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.NETWORK]
    );

    const storyDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.STORY_LOG]
    );

    const productDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.PRODUCT]
    );

    await parseLog(invenDataList, (invenData) => invenData.logDate, COLLECTION_LIST.INVEN);
    await parseLog(loginDataList, (loginData) => loginData.loginDate, COLLECTION_LIST.LOGIN);
    await parseLog(networkDataList, (networkData) => networkData.res.common.serverTime, COLLECTION_LIST.NETWORK);
    await parseLog(storyDataList, (storyData) => storyData.updateDate, COLLECTION_LIST.STORY_LOG);
    await parseLog(productDataList, (productData) => productData.purchaseDate, COLLECTION_LIST.PRODUCT);

    if (invenDataList.length || loginDataList.length || networkDataList.length || storyDataList.length || productDataList.length)
        setTimeout(() => {eventEmitter.emit('processing');}, 1000);
    else 
        console.log('collect end');
        
});

// testFuncDrop("inven");
// testFuncDrop("login");
// testFuncDrop("network");
// testFuncDrop("story_log");
// testFuncDrop("product");

async function testFuncDrop(prefix) {
    const connection = await MongoConnectionHelper.setConnection(configs.dbMongoLog);
    const db = connection.db(SOURCE_DB);
    
    for(let i = 30 ; i >= 0; i--) {
        const YYYYMMDD = DateUtil.utsToDs(moment().subtract(i, "days").unix(), DateUtil.YYYYMMDD);
        const collectionName = `${prefix}_${YYYYMMDD}`;
        try {
            await db.dropCollection(collectionName);
            console.log(`${collectionName} - drop`);
        }
        catch(err) {
            console.log(`${collectionName} - drop err`);
        }
    }
}

async function start() {
    const connection = await MongoConnectionHelper.setConnection(configs.dbMongoLog);
    const db = connection.db(SOURCE_DB);

    targetDb = connection.db(TARGET_DB);

    const sourceList = Object.values(COLLECTION_LIST);

    for (const source of sourceList) {
        sourceDbConnections[source] = db.collection(source);
    }

    eventEmitter.emit('processing');
}

async function collecting(connection) {
    return await connection.find({}, { limit: 100 }).toArray();
}

async function parseLog(dataList, parseDate, prefix) {
    const collectionMap = {}
    for (const data of dataList) {
        const YYYYMMDD = DateUtil.utsToDs(parseDate(data), DateUtil.YYYYMMDD);
        if (!collectionMap[YYYYMMDD])
            collectionMap[YYYYMMDD] = [];

        collectionMap[YYYYMMDD].push(data);
    }

    const collectionsKeys = Object.keys(collectionMap);
    for (const collectionKey of collectionsKeys) {
        const collectionName = `${prefix}_${collectionKey}`;
        const collectionDataList = collectionMap[collectionKey];

        const dbCollection = targetDb.collection(collectionName);
        await dbCollection.insertMany(collectionDataList);
    }

    const collection = sourceDbConnections[prefix];

    for(const data of dataList) {
        await collection.deleteOne({_id: data._id});
    }
}


start();