const nodeEnv = process.env.NODE_ENV;

const dbMongoLog = require(`./configs/${nodeEnv}/dbMongoLog.json`);

module.exports = {
    dbMongoLog
}