import fs from 'fs';
import path from 'path';
import { MongoClient, Db } from 'mongodb';

import { restoreCollection } from './restore-collection';
import { maybeConnectServer, verifyCollectionsDontExist } from './utils';

interface RestoreDatabaseOptions {
    con?: MongoClient;
    uri?: string;
    database: string;
    from: string; // Path to the folder containing .bson files
    clean?: boolean;
    onCollectionExists?: 'throw' | 'overwrite';
    transformDocuments?: (doc: any, info: { collection: string }) => any;
}

const checkOptions = (bag: Partial<RestoreDatabaseOptions>): void => {
    const {
        con,
        uri,
        database,
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

const doRestoreDatabase = async (bag: RestoreDatabaseOptions): Promise<void> => {
    const {
        con,
        uri,
        database,
        from,
        clean = true,
        onCollectionExists = 'throw',
        transformDocuments,
    } = bag;

    const bsonRX = /\.bson$/;

    const serverConnection = await maybeConnectServer({ con, uri });
    const dbHandle: Db = serverConnection.db(database);

    const collections = fs.readdirSync(from)
        .filter((filename) => bsonRX.test(filename))
        .map((filename) => ({
            filename,
            mongoname: filename.replace(bsonRX, ''),
        }));

    try {
        await verifyCollectionsDontExist({
            dbHandle,
            collections: collections.map(it => it.mongoname),
            onCollectionExists,
        });

        await Promise.all(
            collections.map(({ filename, mongoname }) => {
                let wrappedTransform: ((doc: any) => any) | undefined;

                if (transformDocuments) {
                    wrappedTransform = (doc: any) =>
                        transformDocuments(doc, { collection: mongoname });
                }

                return restoreCollection({
                    con: serverConnection,
                    database,
                    collection: mongoname,
                    from: path.join(from, filename),
                    clean,
                    onCollectionExists,
                    transformDocuments: wrappedTransform,
                });
            })
        );
    } finally {
        if (!con) {
            await serverConnection.close();
        }
    }
};

export const restoreDatabase = (options: RestoreDatabaseOptions): Promise<void> => {
    checkOptions(options);
    return doRestoreDatabase(options);
};
