package org.polypheny.db.mql;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.polypheny.db.mql.parser.MqlParserPos;

@Accessors(fluent = true)
public abstract class MqlNode {

    @Getter
    @Setter
    List<String> stores = new ArrayList<>();

    @Setter
    @Getter
    List<String> primary = new ArrayList<>();


    protected BsonDocument getDocumentOrNull( BsonDocument document, String name ) {
        if ( document != null && document.containsKey( name ) && document.get( name ).isDocument() ) {
            return document.getDocument( name );
        } else {
            return null;
        }
    }


    protected BsonArray getArrayOrNull( BsonDocument document, String name ) {
        if ( document != null && document.containsKey( name ) && document.get( name ).isArray() ) {
            return document.getBoolean( name ).asArray();
        } else {
            return null;
        }
    }


    protected boolean getBoolean( BsonDocument document, String name ) {
        if ( document != null && document.containsKey( name ) && document.get( name ).isBoolean() ) {
            return document.getBoolean( name ).asBoolean().getValue();
        } else {
            return false;
        }
    }


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
