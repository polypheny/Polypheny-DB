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
import org.polypheny.db.adapter.mongodb.util.MongoDynamic;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.type.PolyType;

@Getter
public class BsonDynamic extends BsonDocument {

    private final long id;


    public BsonDynamic( RexDynamicParam rexNode ) {
        this( rexNode.getIndex(), getTypeString( rexNode.getType() ) );
    }


    private static String getTypeString( AlgDataType type ) {
        String textualType = type.getPolyType() != PolyType.ARRAY
                ? type.getPolyType().getTypeName()
                : "ARRAY$" + getTypeString( type.getComponentType() );
        return textualType + (type.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED ? "" : "|" + type.getPrecision());
    }


    public BsonDynamic( long id, String polyTypeName ) {
        super();
        this.id = id;
        append( MongoDynamic.MONGO_DYNAMIC_INDEX_KEY, new BsonInt64( id ) );
        append( MongoDynamic.MONGO_DYNAMIC_TYPE_KEY, new BsonString( polyTypeName ) );
        append( MongoDynamic.MONGO_DYNAMIC_REGEX_KEY, new BsonBoolean( false ) );
        append( MongoDynamic.MONGO_DYNAMIC_FUNC_BODY_KEY, new BsonBoolean( false ) );
        append( MongoDynamic.MONGO_DYNAMIC_FUNC_NAME_KEY, BsonNull.VALUE );
    }


    public static BsonDocument addFunction( BsonDocument doc, String functionName ) {
        return doc.append( MongoDynamic.MONGO_DYNAMIC_FUNC_NAME_KEY, new BsonString( functionName ) );
    }


    public static BsonDocument changeType( BsonDocument doc, PolyType polyType ) {
        return doc.append( MongoDynamic.MONGO_DYNAMIC_TYPE_KEY, new BsonString( polyType.getTypeName() ) );
    }


    public BsonDynamic setIsRegex( boolean isRegex ) {
        append( MongoDynamic.MONGO_DYNAMIC_REGEX_KEY, new BsonBoolean( isRegex ) );
        return this;
    }


    public boolean isRegex() {
        return getBoolean( MongoDynamic.MONGO_DYNAMIC_REGEX_KEY ).getValue();
    }


    public BsonDynamic setIsFunc( boolean isFunc ) {
        append( MongoDynamic.MONGO_DYNAMIC_FUNC_BODY_KEY, new BsonBoolean( isFunc ) );
        return this;
    }


    public BsonDynamic setPolyFunction( String functionName ) {
        append( MongoDynamic.MONGO_DYNAMIC_FUNC_NAME_KEY, new BsonString( functionName ) );
        return this;
    }


    public BsonDynamic adjustType( PolyType polyType ) {
        append( MongoDynamic.MONGO_DYNAMIC_TYPE_KEY, new BsonString( polyType.getTypeName() ) );
        return this;
    }

}
