package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlCount extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final boolean isEstimate;
    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonDocument options;


    public MqlCount( String collection, BsonDocument query, BsonDocument options ) {
        this( collection, query, options, false );
    }


    public MqlCount( String collection, BsonDocument query, BsonDocument options, boolean isEstimate ) {
        super( collection );
        this.query = query;
        this.options = options;
        this.isEstimate = isEstimate;
    }


    @Override
    public Type getKind() {
        return Type.COUNT;
    }

}
