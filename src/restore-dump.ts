'use strict';
import fs from 'fs';
import path from 'path';
import { MongoClient } from 'mongodb';

import { restoreDatabase } from './restore-database';
import { maybeConnectServer } from './utils';

interface RestoreDumpOptions {
    con?: MongoClient;
    uri?: string;
    from: string; // Path to the root dump folder
    clean?: boolean;
    onCollectionExists?: 'throw' | 'overwrite';
    transformDocuments?: (doc: any, info: { database: string }) => any;
}

const checkOptions = (bag: Partial<RestoreDumpOptions>): void => {
    const { con, uri, from, onCollectionExists } = bag;

    if (!con && !uri) {
        throw new Error('neither "con" nor "uri" option was given');
    }

    if (con && uri) {
        throw new Error('you cannot use both "uri" and "con" option');
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

const doRestoreDump = async (bag: RestoreDumpOptions): Promise<void> => {
    const {
        con,
        uri,
        from,
        clean = true,
        onCollectionExists = 'throw',
        transformDocuments,
    } = bag;

    const serverConnection = await maybeConnectServer({ con, uri });

    const databases = fs.readdirSync(from)
        .map(filename => ({
            name: filename,
            path: path.join(from, filename),
        }))
        .filter(entry => fs.statSync(entry.path).isDirectory());

    try {
        // TODO: handle erroneous database restores properly
        await Promise.all(
            databases.map(({ name, path }) => {
                let wrappedTransform: ((doc: any) => any) | undefined;

                if (transformDocuments) {
                    wrappedTransform = (doc: any) =>
                        transformDocuments(doc, { database: name });
                }

                return restoreDatabase({
                    con: serverConnection,
                    database: name,
                    from: path,
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

export const restoreDump = (options: RestoreDumpOptions): Promise<void> => {
    checkOptions(options);
    return doRestoreDump(options);
};
