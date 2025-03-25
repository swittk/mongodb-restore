import fs from 'fs';
import { MongoClient } from 'mongodb';
import { maybeConnectServer, internalRestoreBuffer, tryCreateCollection } from './utils';
import { restoreBuffer } from './restore-buffer';

interface RestoreCollectionOptions {
    con?: MongoClient;
    uri?: string;
    database: string;
    collection: string;
    from: string; // path to BSON file
    limit?: number;
    clean?: boolean;
    onCollectionExists?: 'throw' | 'overwrite';
    filterDocuments?: (doc: any) => boolean;
    transformDocuments?: (doc: any) => any;
}

const checkOptions = (bag: Partial<RestoreCollectionOptions>): void => {
    const {
        con,
        uri,
        database,
        collection,
        from,
        onCollectionExists,
    } = bag;

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
        onCollectionExists &&
        !['throw', 'overwrite'].includes(onCollectionExists)
    ) {
        throw new Error(
            'when set "onCollectionExists" should be either "throw" or "overwrite"'
        );
    }
};

const doRestoreCollection = async (bag: RestoreCollectionOptions): Promise<void> => {
    const {
        con,
        uri,
        database,
        collection,
        from,
        limit,
        clean = true,
        onCollectionExists = 'throw',
        filterDocuments,
        transformDocuments,
    } = bag;

    // FIXME: this will blow up on large collections
    const buffer = fs.readFileSync(from);

    await restoreBuffer({
        con,
        uri,
        database,
        collection,
        from: buffer,
        limit,
        clean,
        onCollectionExists,
        filterDocuments,
        transformDocuments,
    });
};

export const restoreCollection = (options: RestoreCollectionOptions): Promise<void> => {
    checkOptions(options);
    return doRestoreCollection(options);
};