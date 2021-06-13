package org.polypheny.db.mql;

import org.polypheny.db.mql.Mql.Type;

public class MqlCreateCollection extends MqlNode {

    String name;


    public MqlCreateCollection( String name ) {
        this.name = name;
    }


    @Override
    Type getKind() {
        return Type.CREATE_COLLECTION;
    }


    @Override
    public String toString() {
        return "MqlCreateCollection{" +
                "name='" + name + '\'' +
                '}';
    }

}
