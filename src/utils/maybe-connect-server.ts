import { MongoClient, MongoClientOptions } from 'mongodb';

export interface MaybeConnectServerBag {
    con?: MongoClient;
    uri?: string;
}

export const maybeConnectServer = async (bag: MaybeConnectServerBag): Promise<MongoClient> => {
    const { con, uri } = bag;

    let serverConnection: MongoClient;
    if (!con) {
        if (!uri) {
            throw new Error("If no connection is provided, please provide a URL");
        }
        const options: MongoClientOptions = {
            // `useUnifiedTopology` is deprecated in recent MongoDB drivers (4.0+), 
            // so it's safe to omit unless using older versions.
        };
        serverConnection = await MongoClient.connect(uri, options);
        // serverConnection = await MongoClient.connect(
        //     uri,
        //     { useUnifiedTopology: true }
        // );
    }
    else {
        serverConnection = con;
    }
    return serverConnection;
}
