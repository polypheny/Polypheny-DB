package org.polypheny.db.mql;

import org.polypheny.db.mql.parser.MqlParserPos;

public abstract class MqlNode {


    public MqlNode() {
    }


    abstract Mql.Type getKind();


    Mql.Family getFamily() {
        return Mql.getFamily( getKind() );
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{}";
    }


    public MqlParserPos getParserPosition() {
        return new MqlParserPos( 0, 0, 0, 0 ); // todo dl fix
    }

}
