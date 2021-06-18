package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlRemove extends MqlCollectionStatement {

    private final BsonDocument document;


    public MqlRemove( String collection, BsonDocument document ) {
        super( collection );
        this.document = document;
    }


    @Override
    public Type getKind() {
        return Type.REMOVE;
    }

}
