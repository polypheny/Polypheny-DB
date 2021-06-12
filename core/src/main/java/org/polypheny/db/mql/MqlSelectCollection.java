package org.polypheny.db.mql;

public class MqlSelectCollection extends MqlNode {

    String name;


    public MqlSelectCollection( String name ) {
        this.name = name;
    }


    @Override
    public String toString() {
        return "MqlSelectCollection{" +
                "name='" + name + '\'' +
                '}';
    }

}
