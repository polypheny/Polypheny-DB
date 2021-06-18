package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlFind extends MqlCollectionStatement {

    @Getter
    private final BsonDocument query;

    @Getter
    private final BsonDocument projection;


    public MqlFind( String collection, BsonDocument query, BsonDocument projection ) {
        super( collection );
        this.query = query;
        this.projection = projection;
    }


    @Override
    public Type getKind() {
        return Type.FIND;
    }

}
