package org.polypheny.db.mql;

public class MqlCreateCollection extends MqlNode {

    String name;


    public MqlCreateCollection( String name ) {
        this.name = name;
    }


    @Override
    public String toString() {
        return "MqlCreateCollection{" +
                "name='" + name + '\'' +
                '}';
    }

}
