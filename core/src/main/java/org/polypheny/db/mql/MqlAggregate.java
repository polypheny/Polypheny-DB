package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlAggregate extends MqlCollectionStatement {

    @Getter
    private final BsonArray pipeline;
    @Getter
    private final BsonDocument option;


    public MqlAggregate( String collection, BsonArray pipeline, BsonDocument option ) {
        super( collection );
        this.pipeline = pipeline;
        this.option = option;

        enforceNonEmptyProject( pipeline );

    }

    private void enforceNonEmptyProject( BsonArray pipeline) {
        
    }


    @Override
    public Type getKind() {
        return Type.AGGREGATE;
    }

}
