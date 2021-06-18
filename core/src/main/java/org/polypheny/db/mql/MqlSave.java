package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlSave extends MqlCollectionStatement {

    private final BsonDocument document;


    public MqlSave( String collection, BsonDocument document ) {
        super( collection );
        this.document = document;
    }


    @Override
    public Type getKind() {
        return Type.SAVE;
    }

}
