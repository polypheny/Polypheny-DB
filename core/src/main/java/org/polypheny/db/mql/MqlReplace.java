package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlReplace extends MqlCollectionStatement {

    public MqlReplace( String collection, BsonDocument document ) {
        super( collection, document );
    }


    @Override
    Type getKind() {
        return Type.REPLACE;
    }

}
