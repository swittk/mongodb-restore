import * as BSON from 'bson';

import { Collection, Document } from 'mongodb';

export interface RestoreBufferBag {
    collectionHandle: Collection;
    buffer: Buffer;
    limit?: number;
    filterDocuments?: (doc: Document) => boolean;
    transformDocuments?: (doc: Document) => Document;
}

export const internalRestoreBuffer = async (bag: RestoreBufferBag): Promise<void> => {
    let {
        collectionHandle,

        buffer,
        limit,
        // chunked = false
        filterDocuments,
        transformDocuments,
    } = bag;

    let index = 0;
    let documents: BSON.Document[] = [];
    while (
        buffer.length > index
        && (!limit || limit > documents.length)
    ) {
        var tmp = [];
        index = BSON.deserializeStream(
            buffer, // bsonBuffer
            index,  // deserializationStartIndex,
            1,      // numberOfDocuments
            tmp,    // targetArray
            0,      // targetArrayStartIndex,
            {}   // options
        );
        documents.push(...(
            filterDocuments
                ? tmp.filter(filterDocuments)
                : tmp
        ));
    }

    if (transformDocuments) {
        documents = documents.map(transformDocuments);
    }

    if (documents.length > 0) {
        await collectionHandle.insertMany(documents);
    }
}