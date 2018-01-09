'use strict';

const fs = require('fs');
const promisify = require('util.promisify');

module.exports = {
    writeFile: promisify(fs.writeFile),
    readFile: promisify(fs.readFile),
    unlink: promisify(fs.unlink)
};