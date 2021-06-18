package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlCreateView extends MqlCollectionStatement {

    private final String source;
    private final BsonDocument document;


    public MqlCreateView( String collection, String source, BsonDocument document ) {
        super( collection );
        this.document = document;
        this.source = source;
    }


    @Override
    public Type getKind() {
        return Type.CREATE_VIEW;
    }

}
