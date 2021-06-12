package org.polypheny.db.mql;

public abstract class MqlNode {


    public MqlNode() {
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{}";
    }

}
