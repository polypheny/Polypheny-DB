package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlInsert extends MqlCollectionStatement {

    @Getter
    private final BsonArray array;
    @Getter
    private final boolean ordered;


    public MqlInsert( String collection, BsonArray array, BsonDocument options ) {
        super( collection );
        this.array = array;
        this.ordered = getBoolean( options, "ordered" );
    }


    @Override
    public Type getKind() {
        return Type.INSERT;
    }

}
