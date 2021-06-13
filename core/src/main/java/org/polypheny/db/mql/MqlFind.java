package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlFind extends MqlCollectionStatement {

    public MqlFind( String collection, BsonDocument document ) {
        super( collection, document );
    }


    @Override
    public Type getKind() {
        return Type.FIND;
    }

}
