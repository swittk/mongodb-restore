'use strict';
var fs = require('fs');

var {
    maybeConnectServer,
    internalRestoreBuffer,
    tryCreateCollection
} = require('./utils');

var restoreBuffer = require('./restore-buffer');

module.exports = (options) => {
    checkOptions(options);
    return doRestoreCollection(options);
}

var doRestoreCollection = async ({
    con,
    uri,
    database,
    collection,
    from,

    limit,
    clean = true,
    onCollectionExists = 'throw',
    transformDocuments,
}) => {
    // FIXME: this will blow up on large collections
    var buffer = fs.readFileSync(from);

    await restoreBuffer({
        con,
        uri,
        database,
        collection,
        from: buffer,

        limit,
        clean,
        onCollectionExists,
        transformDocuments,
    });
};


var checkOptions = ({
    con,
    uri,
    database,
    collection,
    from,
    onCollectionExists,
}) => {
    if (!con && !uri) {
        throw new Error('neither "con" nor "uri" option was given');
    }

    if (con && uri) {
        throw new Error('you cannot use both "uri" and "con" option');
    }

    if (!database) {
        throw new Error('missing "database" option');
    }

    if (!collection) {
        throw new Error('missing "collection" option');
    }

    if (!from) {
        throw new Error('missing "from" option');
    }

    if (
        onCollectionExists
        && !['throw', 'overwrite'].includes(onCollectionExists)
    ) {
        throw new Error('when set "onCollectionExists" should be either "throw" or "overwrite"');
    }
}
