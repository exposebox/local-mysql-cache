'use strict';

const _ = require('underscore');
const changeCase = require('change-case');
const EventEmitter = require('events');

const fs = require('./fs-promisified');

const debug = require('debug')('local-mysql-cache');

class MySqlCache extends EventEmitter {
    constructor(options) {
        super();

        this.data = new Map();

        this.applyOptions(options);
        this.startCache();
    }

    applyOptions(options) {
        const optionsWithDefaults = Object.assign(this.getDefaults(), options);

        for (let optionKey in optionsWithDefaults) {
            if (optionsWithDefaults[optionKey] == null) {
                throw new Error(`"${optionKey}" option is missing`);
            }
        }

        Object.assign(this, optionsWithDefaults);
    }

    getDefaults() {
        const className = this.constructor.name;

        return {
            sql: undefined,
            mysqlQueryable: undefined,
            cacheFilePath: `./${changeCase.paramCase(className)}.json`,
            name: className,
            keyFieldName: 'id',
            reloadFromMySqlInterval: (5 + Math.random()) * 60 * 1000,
            shouldSaveToFile: false
        };
    }

    startCache() {
        const loadDataPromises = [];

        loadDataPromises.push(this.loadFromDatabaseAndSaveToFile());

        this.reloadTimer = setInterval(() => this.loadFromDatabaseAndSaveToFile(), this.reloadFromMySqlInterval);

        if (this.shouldSaveToFile) {
            loadDataPromises.push(this.loadCacheFromFile());
        }

        this.loadDataPromise = new Promise(resolve => this.on('update', resolve));
    }

    destroy() {
        clearInterval(this.reloadTimer);

        debug('Cache ', this.name, ' destroyed');
    }

    async loadFromDatabaseAndSaveToFile() {
        try {
            await this.loadFromDatabase();

            if (this.shouldSaveToFile) {
                await this.saveToFile();
            }
        } catch (err) {
            this.emit('error', err);
        }
    }

    async loadFromDatabase() {
        const rows = await this.queryDatabase();

        if (rows == null || rows.length === 0) {
            this.setData();

            return;
        }

        const cacheMap = new Map();

        const Item = this.itemClass;

        const mapItems = [];

        if (this.parseDataMultiRow !== undefined) {
            for (let i = 0; i < rows.length; i++) {
                mapItems.push(...this.parseDataMultiRow(rows[i]));
            }
        } else {
            for (let i = 0; i < rows.length; i++) {
                mapItems.push(this.parseDataRow(rows[i]));
            }
        }

        for (let i = 0; i < mapItems.length; i++) {
            const mapItem = mapItems[i];

            const dataKey = mapItem[0];
            let dataValue = mapItem[1];

            const isValueArray = Array.isArray(dataValue);

            if (Item !== undefined) {
                if (!isValueArray) {
                    dataValue = new Item(dataValue);
                } else {
                    for (let j = 0; j < dataValue.length; j++) {
                        dataValue[j] = new Item(dataValue[j]);
                    }
                }
            }

            const mapDataValue = cacheMap.get(dataKey);

            if (!isValueArray) {
                if (mapDataValue === undefined) {
                    cacheMap.set(dataKey, [dataValue]);
                } else {
                    mapDataValue.push(dataValue);
                }
            } else {
                if (mapDataValue === undefined) {
                    cacheMap.set(dataKey, dataValue);
                } else {
                    mapDataValue.push(...dataValue);
                }
            }
        }

        this.setData(cacheMap);
    }

    setData(mapData) {
        if (mapData === undefined) {
            this.data = new Map();
        } else if (mapData instanceof Map) {
            this.data = mapData;
        } else {
            const Item = this.itemClass;

            //  Array of map data
            if (Item !== undefined) {
                for (let mapPair of mapData) {
                    const mapValues = mapPair[1];

                    for (let i = 0; i < mapValues.length; i++) {
                        mapValues[i] = new Item(mapValues[i]);
                    }
                }
            }

            this.data = new Map(mapData);
        }

        this.lastUpdate = new Date();

        debug('Cache', this.name, 'data updated with', this.data.size, 'entries');

        this.emit('update');
    }

    async queryDatabase() {
        debug('Loading cache', this.name, 'from database...');

        const queryDatabasePromise =
            new Promise((resolve, reject) => {
                return this.mysqlQueryable
                    .query(this.sql, [], (err, ...args) => err ? reject(err) : resolve(...args));
            });

        const rows = await queryDatabasePromise;

        debug('Cache', this.name, 'loaded from database with', rows.length, 'entries');

        return rows;
    }

    parseDataRow(row) {
        return [row[this.keyFieldName], row];
    }

    async saveToFile() {
        const dataJson = JSON.stringify([...this.data]);

        await fs.writeFile(this.cacheFilePath, dataJson);

        debug('Cache', this.name, 'saved to file');
    }

    async loadCacheFromFile() {
        debug('Loading cache', this.name, 'from file...');

        try {
            const data = await fs.readFile(this.cacheFilePath);

            const mapData = !_.isEmpty(data) ? JSON.parse(data) : null;

            this.setData(mapData);

            debug('Cache', this.name, 'loaded from file with', mapData.length, 'entries');
        } catch (err) {
            if (err.code === 'ENOENT') {
                debug(`Cache file is missing (${err.message})`);
            } else {
                this.emit('error', err);
            }
        }
    }

    async ensureCacheReady() {
        await this.loadDataPromise;
    }

    getValues(key) {
        const itemValues = this.data.get(key);

        if (itemValues == null || itemValues.length === 0) {
            return [];
        }

        return this.cloneArray(itemValues);
    }

    getFirstValue(key) {
        const itemValues = this.data.get(key);

        if (itemValues == null || itemValues.length === 0) {
            return null;
        }

        return this.cloneArray([itemValues[0]])[0];
    }

    getAll() {
        const items = [];

        for (const itemValues of this.data.values()) {
            items.push(...itemValues);
        }

        return this.cloneArray(items);
    }

    cloneArray(items) {
        if (items.length === 0) {
            return [];
        }

        return JSON.parse(JSON.stringify(items));
    }
}

module.exports = MySqlCache;