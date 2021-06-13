package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlSave extends MqlCollectionStatement {

    public MqlSave( String collection, BsonDocument document ) {
        super( collection, document );
    }


    @Override
    public Type getKind() {
        return Type.SAVE;
    }

}
