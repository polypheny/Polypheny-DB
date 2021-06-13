package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlCreateView extends MqlCollectionStatement {

    private final String source;


    public MqlCreateView( String collection, String source, BsonDocument document ) {
        super( collection, document );
        this.source = source;
    }


    @Override
    Type getKind() {
        return Type.CREATE_VIEW;
    }

}
