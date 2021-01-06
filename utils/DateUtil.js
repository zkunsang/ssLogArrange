const moment = require('moment');

const _ = require('lodash');
const defaultDateFormat = 'YYYY-MM-DD HH:mm:ss';
const YYYYMMDD = 'YYYYMMDD';
class DateUtil {
    constructor() { }

    dsToUtsObj(object, columnName, dateFormat = defaultDateFormat) {
        object[columnName] = moment(object[columnName], dateFormat).unix();
    }

    utsToDsObj(object, columnName) {
        if(!object[columnName])
            return;
            
        const length = object[columnName].toString().length;
        if (length > 10)
            object[columnName] = object[columnName] / 1000;

        object[columnName] = moment.unix(object[columnName]).format(defaultDateFormat);
    }

    dsToUts(ds, dateFormat = defaultDateFormat) {
        return moment(ds, dateFormat).unix();
    }

    utsToDs(uts, dateFormat = defaultDateFormat) {
        const length = uts.toString().length;
        if (length > 10)
            uts = parseInt(uts / 1000);

        return moment.unix(uts).format(dateFormat);
    }

    getStrFromuts(uts) {

    }

    isBetween(now, startDate, endDate) {
        return moment.unix(now / 1000).isBetween(
            moment.unix(startDate),
            moment.unix(endDate));
    }

    getHours() {
        return moment().hours();
    }
}

module.exports = new DateUtil();
module.exports.DEFAULT_FORMAT = defaultDateFormat;
module.exports.YYYYMMDD = YYYYMMDD;