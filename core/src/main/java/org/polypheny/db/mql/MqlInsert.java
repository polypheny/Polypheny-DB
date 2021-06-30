package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonArray;
import org.polypheny.db.mql.Mql.Type;

public class MqlInsert extends MqlCollectionStatement {

    @Getter
    private final BsonArray array;


    public MqlInsert( String collection, BsonArray array ) {
        super( collection );
        this.array = array;
    }


    @Override
    public Type getKind() {
        return Type.INSERT;
    }

}
