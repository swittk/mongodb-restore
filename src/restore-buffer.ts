import { MongoClient } from 'mongodb';
import { maybeConnectServer, internalRestoreBuffer, tryCreateCollection } from './utils';

interface RestoreBufferOptions {
    con?: MongoClient;
    uri?: string;
    database: string;
    collection: string;
    from: Buffer;
    limit?: number;
    clean?: boolean;
    onCollectionExists?: 'throw' | 'overwrite' | ((collection: string) => void);
    filterDocuments?: (doc: any) => boolean;
    transformDocuments?: (doc: any) => any;
}

const checkOptions = (bag: Partial<RestoreBufferOptions>): void => {
    const {
        con,
        uri,
        database,
        collection,
        from,
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
    } else if (!Buffer.isBuffer(from)) {
        throw new Error('value of "from" option must be a buffer');
    }
};

const doRestoreBuffer = async (bag: RestoreBufferOptions): Promise<void> => {
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

    const serverConnection = await maybeConnectServer({ con, uri });

    try {
        const dbHandle = serverConnection.db(database);
        const dbCollection = dbHandle.collection(collection);

        await tryCreateCollection({
            dbHandle,
            collection,
            onCollectionExists,
        });

        if (clean) {
            await dbCollection.deleteMany({});
        }

        await internalRestoreBuffer({
            collectionHandle: dbCollection,
            buffer: from,
            limit,
            filterDocuments,
            transformDocuments,
        });
    } finally {
        if (!con) {
            await serverConnection.close();
        }
    }
};

export const restoreBuffer = (options: RestoreBufferOptions): Promise<void> => {
    checkOptions(options);
    return doRestoreBuffer(options);
};