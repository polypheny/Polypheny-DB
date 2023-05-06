/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.type.entity;

import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.document.PolyBoolean;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializerDef;
import org.polypheny.db.type.entity.document.PolyInteger;
import org.polypheny.db.type.entity.document.PolyList;
import org.polypheny.db.type.entity.document.PolyString;
import org.polypheny.db.type.entity.document.PolyString.PolyStringSerializerDef;

@Value
@NonFinal
public abstract class PolyValue implements Expressible, Comparable<PolyValue>, PolySerializable {

    @Serialize
    public boolean nullable;
    @Serialize
    public PolyType type;


    public static SerializerBuilder getAbstractBuilder() {
        return PolySerializable.builder.get()
                .with( PolyString.class, ctx -> new PolyStringSerializerDef() )
                .with( PolyDocument.class, ctx -> new PolyDocumentSerializerDef() );
    }


    public PolyValue( @Deserialize("type") PolyType type, @Deserialize("nullable") boolean nullable ) {
        this.type = type;
        this.nullable = nullable;
    }


    public static Class<? extends PolyValue> classFrom( PolyType polyType ) {
        switch ( polyType ) {

            case BOOLEAN:
                return PolyBoolean.class;
            case TINYINT:
                break;
            case SMALLINT:
                break;
            case INTEGER:
                return PolyInteger.class;
            case BIGINT:
                break;
            case DECIMAL:
                break;
            case FLOAT:
                break;
            case REAL:
                break;
            case DOUBLE:
                break;
            case DATE:
                break;
            case TIME:
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                break;
            case TIMESTAMP:
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                break;
            case INTERVAL_YEAR:
                break;
            case INTERVAL_YEAR_MONTH:
                break;
            case INTERVAL_MONTH:
                break;
            case INTERVAL_DAY:
                break;
            case INTERVAL_DAY_HOUR:
                break;
            case INTERVAL_DAY_MINUTE:
                break;
            case INTERVAL_DAY_SECOND:
                break;
            case INTERVAL_HOUR:
                break;
            case INTERVAL_HOUR_MINUTE:
                break;
            case INTERVAL_HOUR_SECOND:
                break;
            case INTERVAL_MINUTE:
                break;
            case INTERVAL_MINUTE_SECOND:
                break;
            case INTERVAL_SECOND:
                break;
            case CHAR:
                break;
            case VARCHAR:
                return PolyString.class;
            case BINARY:
                break;
            case VARBINARY:
                break;
            case NULL:
                break;
            case ANY:
                break;
            case SYMBOL:
                break;
            case MULTISET:
                break;
            case ARRAY:
                break;
            case MAP:
                break;
            case DOCUMENT:
                break;
            case GRAPH:
                break;
            case NODE:
                break;
            case EDGE:
                break;
            case PATH:
                break;
            case DISTINCT:
                break;
            case STRUCTURED:
                break;
            case ROW:
                break;
            case OTHER:
                break;
            case CURSOR:
                break;
            case COLUMN_LIST:
                break;
            case DYNAMIC_STAR:
                break;
            case GEOMETRY:
                break;
            case FILE:
                break;
            case IMAGE:
                break;
            case VIDEO:
                break;
            case AUDIO:
                break;
            case JSON:
                break;
        }
        throw new NotImplementedException();
    }


    public boolean isSameType( PolyValue value ) {
        return type == value.type;
    }


    public boolean isBoolean() {
        return type == PolyType.BOOLEAN;
    }


    public PolyBoolean asBoolean() {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        return null;
    }


    public boolean isInteger() {
        return type == PolyType.INTEGER;
    }


    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }
        return null;
    }


    public boolean isDocument() {
        return type == PolyType.DOCUMENT;
    }


    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        return null;
    }


    public boolean isList() {
        return type == PolyType.ARRAY;
    }


    public PolyList asList() {
        if ( isList() ) {
            return (PolyList) this;
        }
        return null;
    }


    public boolean isString() {
        return type == PolyType.VARCHAR;
    }


    public PolyString asString() {
        if ( isString() ) {
            return (PolyString) this;
        }
        return null;
    }


}
