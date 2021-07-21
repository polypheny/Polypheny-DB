package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlDelete extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonDocument options;
    @Getter
    private final boolean onlyOne;


    public MqlDelete( String collection, BsonDocument query, BsonDocument options, boolean onlyOne ) {
        super( collection );
        this.query = query;
        this.options = options;
        this.onlyOne = onlyOne;
    }


    @Override
    public Type getKind() {
        return Type.DELETE;
    }

}
