package org.polypheny.db.mql;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public abstract class MqlCollectionStatement extends MqlNode {

    @Getter
    private final String collection;

    @Setter
    @Getter
    @Accessors(chain = true)
    private int limit = 10;


    public MqlCollectionStatement( String collection ) {
        this.collection = collection;
    }


}
