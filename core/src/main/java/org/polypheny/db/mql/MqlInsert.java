package org.polypheny.db.mql;

import java.util.Collections;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.mql.Mql.Type;

public class MqlInsert extends MqlCollectionStatement {

    @Getter
    private final BsonArray values;
    @Getter
    private final boolean ordered;


    public MqlInsert( String collection, BsonValue values, BsonDocument options ) {
        super( collection );
        if ( values.isDocument() ) {
            this.values = new BsonArray( Collections.singletonList( values.asDocument() ) );
        } else if ( values.isArray() ) {
            this.values = values.asArray();
        } else {
            throw new RuntimeException( "Insert requires either a single document or multiple documents in an array." );
        }
        this.ordered = getBoolean( options, "ordered" );
    }


    @Override
    public Type getKind() {
        return Type.INSERT;
    }

}
