package org.polypheny.db.mql;

import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.polypheny.db.mql.Mql.Type;

public class MqlUpdate extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonArray pipeline;
    @Getter
    private final BsonDocument update;
    @Getter
    private final boolean usesPipeline;
    @Getter
    private final boolean upsert;
    @Getter
    private final boolean multi;
    @Getter
    private final BsonDocument collation;
    @Getter
    private final boolean onlyOne;


    public MqlUpdate( String collection, BsonDocument query, BsonValue updateOrPipeline, BsonDocument options, boolean onlyOne ) {
        super( collection );
        this.query = query;
        if ( updateOrPipeline.isArray() ) {
            this.pipeline = updateOrPipeline.asArray();
            this.update = null;
            this.usesPipeline = true;
        } else {
            this.pipeline = null;
            this.update = updateOrPipeline.asDocument();
            this.usesPipeline = false;
        }
        this.upsert = getBoolean( options, "upsert" );
        this.multi = getBoolean( options, "multi" );
        this.collation = getDocumentOrNull( options, "collation" );
        this.onlyOne = onlyOne;
    }


    @Override
    public Type getKind() {
        return Type.UPDATE;
    }

}
