/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.mongodb.bson;

import lombok.Getter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.type.PolyType;

public class BsonDynamic extends BsonDocument {

    @Getter
    private final long id;


    public BsonDynamic( RexDynamicParam rexNode ) {
        this( rexNode.getIndex(), getTypeString( rexNode.getType() ) );
    }


    private static String getTypeString( AlgDataType type ) {
        return type.getPolyType() != PolyType.ARRAY
                ? type.getPolyType().getTypeName()
                : "ARRAY$" + getTypeString( type.getComponentType() );
    }


    public BsonDynamic( long id, String polyTypeName ) {
        super();
        this.id = id;
        append( "_dyn", new BsonInt64( id ) );
        append( "_type", new BsonString( polyTypeName ) );
        append( "_reg", new BsonBoolean( false ) );
        append( "_func", new BsonBoolean( false ) );
        append( "_functionName", BsonNull.VALUE );
    }


    public static BsonDocument addFunction( BsonDocument doc, String functionName ) {
        return doc.append( "_functionName", new BsonString( functionName ) );
    }


    public static BsonDocument changeType( BsonDocument doc, PolyType polyType ) {
        return doc.append( "_type", new BsonString( polyType.getTypeName() ) );
    }


    public BsonDynamic setIsRegex( boolean isRegex ) {
        append( "_reg", new BsonBoolean( isRegex ) );
        return this;
    }


    public boolean isRegex() {
        return getBoolean( "_reg" ).getValue();
    }


    public BsonDynamic setIsFunc( boolean isFunc ) {
        append( "_func", new BsonBoolean( isFunc ) );
        return this;
    }


    public BsonDynamic setPolyFunction( String functionName ) {
        append( "_functionName", new BsonString( functionName ) );
        return this;
    }


    public BsonDynamic adjustType( PolyType polyType ) {
        append( "_type", new BsonString( polyType.getTypeName() ) );
        return this;
    }

}
