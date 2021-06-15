package org.polypheny.db.mql;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.mql.parser.MqlParserPos;

@Accessors(fluent = true)
public abstract class MqlNode {

    @Getter
    @Setter
    List<String> stores;

    @Setter
    @Getter
    List<String> primary;


    public abstract Mql.Type getKind();


    public Mql.Family getFamily() {
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
