import { Db, ReadPreference } from 'mongodb';
import { CollectionsExist } from '../errors';

export interface VerifyCollectionsDontExistBag {
    dbHandle: Db;
    collections: string[];
    onCollectionExists?: 'overwrite' | ((collection: string) => void) | 'throw';
}

export const verifyCollectionsDontExist = async (
    bag: VerifyCollectionsDontExistBag
): Promise<boolean> => {
    const { dbHandle, collections, onCollectionExists } = bag;

    // see: node-mongodb-native (v4.17.1)
    //      /src/operations/options_operation.ts:30
    const existing = await dbHandle
        .listCollections(
            { name: { $in: collections } },
            { readPreference: ReadPreference.PRIMARY }
        )
        .toArray();

    if (existing.length > 0) {
        if (onCollectionExists !== 'overwrite') {
            throw new CollectionsExist({
                collections: existing.map(it => it.name),
            });
        }
        return false;
    } else {
        return true;
    }
};
