import { Db } from 'mongodb';
import { verifyCollectionsDontExist } from './verify-collections-dont-exist';

export interface TryCreateCollectionBag {
    dbHandle: Db;
    collection: string;
    onCollectionExists?: ((collection: string) => void) | 'overwrite' | 'throw';
}

export const tryCreateCollection = async (bag: TryCreateCollectionBag): Promise<void> => {
    const { dbHandle, collection, onCollectionExists } = bag;

    const doesntExist = await verifyCollectionsDontExist({
        dbHandle,
        collections: [collection],
        onCollectionExists
    });

    if (doesntExist) {
        await dbHandle.createCollection(collection);
    }
};