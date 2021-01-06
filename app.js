const MongoConnectionHelper = require('./MongoConnectionHelper');
const configs = require('./configs');
const events = require('events');
const DateUtil = require('./utils/DateUtil');
const eventEmitter = new events.EventEmitter();

const { ObjectID } = require('mongodb');

let sourceDbConnections = {};
let sourceLastObjectId = {};
let targetDb = null;

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
        sourceDbConnections[COLLECTION_LIST.INVEN],
        createWherePhrase(COLLECTION_LIST.INVEN)
    );

    const loginDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.LOGIN],
        createWherePhrase(COLLECTION_LIST.LOGIN)
    );

    const networkDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.NETWORK],
        createWherePhrase(COLLECTION_LIST.NETWORK)
    );

    const storyDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.STORY_LOG],
        createWherePhrase(COLLECTION_LIST.STORY_LOG)
    );

    const productDataList = await collecting(
        sourceDbConnections[COLLECTION_LIST.PRODUCT],
        createWherePhrase(COLLECTION_LIST.PRODUCT)
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

function createWherePhrase(collectionName) {
    return sourceLastObjectId[collectionName] ? { "_id": { "$gt": ObjectID(sourceLastObjectId[collectionName]) } } : {}
}

async function collecting(connection, wherePhrase) {
    return await connection.find(wherePhrase, { limit: 100 }).toArray();
}

async function parseLog(dataList, parseDate, prefix) {
    const collectionMap = {}
    for (const data of dataList) {
        const YYYYMMDD = DateUtil.utsToDs(parseDate(data), DateUtil.YYYYMMDD);
        if (!collectionMap[YYYYMMDD])
            collectionMap[YYYYMMDD] = [];

        collectionMap[YYYYMMDD].push(data);

        if (!sourceLastObjectId[prefix] || sourceLastObjectId[prefix] < data._id.toString())
            sourceLastObjectId[prefix] = data._id.toString();
    }

    const collectionsKeys = Object.keys(collectionMap);
    for (const collectionKey of collectionsKeys) {
        const collectionName = `${prefix}_${collectionKey}`;
        const collectionDataList = collectionMap[collectionKey];

        const dbCollection = targetDb.collection(collectionName);
        await dbCollection.insertMany(collectionDataList);
    }
}


start();