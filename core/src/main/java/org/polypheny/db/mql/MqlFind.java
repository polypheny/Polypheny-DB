package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlFind extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final BsonDocument query;

    @Getter
    private final BsonDocument projection;
    @Getter
    private final boolean onlyOne;


    public MqlFind( String collection, BsonDocument query, BsonDocument projection, boolean onlyOne ) {
        super( collection );
        this.query = query != null ? query : new BsonDocument();
        this.projection = projection != null ? projection : new BsonDocument();
        this.onlyOne = onlyOne;
    }


    @Override
    public Type getKind() {
        return Type.FIND;
    }

}
