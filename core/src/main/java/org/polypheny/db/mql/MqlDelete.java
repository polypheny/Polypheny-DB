package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlDelete extends MqlCollectionStatement {

    private final BsonDocument document;


    public MqlDelete( String collection, BsonDocument document ) {
        super( collection );
        this.document = document;
    }


    @Override
    public Type getKind() {
        return Type.DELETE;
    }

}
