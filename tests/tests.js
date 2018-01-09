'use strict';

const mysql = require('mysql');
const should = require('should');
const promisify = require('util.promisify');
const fs = require('../src/fs-promisified');
const config = require('./config');

const MysqlCache = require('../src/mysql-cache');

const MysqlPool = require('mysql/lib/Pool');

MysqlPool.prototype.queryAsync = promisify(MysqlPool.prototype.query);
MysqlPool.prototype.endAsync = promisify(MysqlPool.prototype.end);

class TestRow {
    constructor(data) {
        Object.assign(this, data);
    }
}

describe('local mysql cache', function () {
    this.timeout(10 * 1000);

    before(async function () {
        const tableName = `test_${Date.now()}`;

        const mysqlPool = mysql.createPool(config.mysqlPool);

        await mysqlPool
            .queryAsync(
                `CREATE TABLE ${tableName} (
                    id INT(11) NOT NULL,
                    text VARCHAR(50) NULL DEFAULT NULL,
                    PRIMARY KEY (id, text)
                )
                COLLATE='utf8_unicode_ci'
                ENGINE=InnoDB;`);

        const insertValues = [];

        for (let i = 0; i < 100; i++) {
            for (let j = 0; j < 3; j++) {
                insertValues.push(`(${i}, "${j}_${getRandomNumber().toString(36)}")`);
            }
        }

        await mysqlPool.queryAsync(
            `INSERT INTO ${tableName} (id, text) VALUES ${insertValues.join(',')}`);

        this.tableName = tableName;
        this.mysqlPool = mysqlPool;
    });

    after(async function () {
        const mysqlPool = this.mysqlPool;

        if (mysqlPool) {
            await mysqlPool.queryAsync(`DROP TABLE ${this.tableName}`);
            await mysqlPool.end();
        }
    });

    describe('general', function () {
        before(async function () {
            this.mysqlCache = createMySqlCache(this);
        });

        after(async function () {
            const mysqlCache = this.mysqlCache;

            if (mysqlCache) {
                mysqlCache.destroy();
            }
        });

        it('load all items', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getAll();

            should.equal(items.length, 300);
        });

        it('load out of bound item', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const item = mysqlCache.getFirstValue(150);

            should.not.exist(item);
        });

        it('load one item', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const item = mysqlCache.getFirstValue(50);

            should.exist(item);
        });

        it('load multiple items with the same key', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getValues(50);

            should.equal(items.length, 3);
        });
    });

    describe('after file corruption', function () {
        before(async function () {
            await fs.writeFile('./my-sql-cache.json', 'abcd123456!@#$%^&*()');

            this.mysqlCache = createMySqlCache(this, {
                shouldSaveToFile: true
            });
        });

        after(async function () {
            const mysqlCache = this.mysqlCache;

            if (mysqlCache) {
                mysqlCache.destroy();
            }

            await fs.unlink(mysqlCache.cacheFilePath);
        });

        it('load all items', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getAll();

            should.equal(items.length, 300);
        });
    });

    describe('multiple records handling (parseDataRow)', function () {
        before(async function () {
            this.mysqlCache = createMySqlCache(this, {
                shouldSaveToFile: false,
                parseDataRow: function (row) {
                    return [row[this.keyFieldName], [{t1: row}, {t2: row}, {t3: row}]];
                }
            });
        });

        after(async function () {
            const mysqlCache = this.mysqlCache;

            if (mysqlCache) {
                mysqlCache.destroy();
            }
        });

        it('load all items', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getAll();

            should.equal(items.length, 900);
        });

        it('load all items', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getValues(50);

            should.equal(items.length, 9);
        });
    });

    describe('multiple records handling (parseDataMultiRow)', function () {
        before(async function () {
            this.mysqlCache = createMySqlCache(this, {
                shouldSaveToFile: false,
                parseDataMultiRow: function (row) {
                    const keyFieldName = this.keyFieldName;

                    return [
                        [row[keyFieldName], {t1: row}],
                        [row[keyFieldName], {t2: row}],
                        [row[keyFieldName], {t3: row}]
                    ];
                }
            });
        });

        after(async function () {
            const mysqlCache = this.mysqlCache;

            if (mysqlCache) {
                mysqlCache.destroy();
            }
        });

        it('load all items', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getAll();

            should.equal(items.length, 900);
        });

        it('load all items', async function () {
            let mysqlCache = this.mysqlCache;

            await mysqlCache.ensureCacheReady();

            const items = mysqlCache.getValues(50);

            should.equal(items.length, 9);
        });
    });
});

function createMySqlCache(context, options) {
    return new MysqlCache(Object.assign({
        sql: `SELECT id, text FROM ${context.tableName}`,
        mysqlQueryable: context.mysqlPool,
        itemClass: TestRow
    }, options))
        .on('error', err => console.error(err.stack));
}

function getRandomNumber() {
    return Math.trunc(Math.random() * 100000);
}