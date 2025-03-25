export class RestoreError extends Error { };

interface CollectionsExistBag {
    collections?: string[];
}

export class CollectionsExist extends RestoreError {
    collections: string[];
    constructor(bag: CollectionsExistBag = {}) {
        const { collections = [] } = bag;
        super(
            `collections [ ${collections.join(',')} ] already exist;` +
            ` set onCollectionExists to "overwrite" to remove this error`
        );
        this.name = 'RestoreError.CollectionsExist';
        this.collections = collections;
        // Required for extending built-ins like Error in TypeScript in ES5/ES6 targets
        Object.setPrototypeOf(this, CollectionsExist.prototype);
    }
}