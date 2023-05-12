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

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SerializerBuilder;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.document.PolyBoolean;
import org.polypheny.db.type.entity.document.PolyDate;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializerDef;
import org.polypheny.db.type.entity.document.PolyDouble;
import org.polypheny.db.type.entity.document.PolyDouble.PolyDoubleSerializerDef;
import org.polypheny.db.type.entity.document.PolyFloat;
import org.polypheny.db.type.entity.document.PolyFloat.PolyFloatSerializerDef;
import org.polypheny.db.type.entity.document.PolyInteger;
import org.polypheny.db.type.entity.document.PolyInteger.PolyIntegerSerializerDef;
import org.polypheny.db.type.entity.document.PolyList;
import org.polypheny.db.type.entity.document.PolyLong;
import org.polypheny.db.type.entity.document.PolyString;
import org.polypheny.db.type.entity.document.PolyString.PolyStringSerializerDef;
import org.polypheny.db.type.entity.document.PolyTime;
import org.polypheny.db.type.entity.document.PolyTimeStamp;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;

@Value
@EqualsAndHashCode
@NonFinal
public abstract class PolyValue implements Expressible, Comparable<PolyValue>, PolySerializable {

    @NonFinal
    @EqualsAndHashCode.Exclude
    public BinarySerializer<PolyValue> serializer = PolyValue.getAbstractBuilder().build( PolyValue.class );

    @Serialize
    public boolean nullable;
    @Serialize
    public PolyType type;


    public static SerializerBuilder getAbstractBuilder() {

        return PolySerializable.builder.get()
                .with( PolyDocument.class, ctx -> new PolyDocumentSerializerDef() )
                .with( PolyString.class, ctx -> new PolyStringSerializerDef() )
                .with( PolyValue.class, ctx -> new PolyValueSerializerDef() )
                .with( PolyInteger.class, ctx -> new PolyIntegerSerializerDef() )
                .with( PolyDouble.class, ctx -> new PolyDoubleSerializerDef() )
                .with( PolyFloat.class, ctx -> new PolyFloatSerializerDef() );
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
                return PolyFloat.class;
            case REAL:
                break;
            case DOUBLE:
                return PolyDouble.class;
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


    public static PolyValue deserialize( String json ) {
        return PolySerializable.deserialize( json, PolyValue.class );
    }


    public static PolyValue deserialize( PolyType type, String json ) {
        switch ( type ) {
            case BOOLEAN:
                return PolySerializable.deserialize( json, getAbstractBuilder().build( PolyBoolean.class ) );
            case VARCHAR:
                return PolySerializable.deserialize( json, getAbstractBuilder().build( PolyString.class ) );
            case DOCUMENT:
                return PolySerializable.deserialize( json, getAbstractBuilder().build( PolyDocument.class ) );
            case FLOAT:
                return PolySerializable.deserialize( json, getAbstractBuilder().build( PolyFloat.class ) );
            case DOUBLE:
                return PolySerializable.deserialize( json, getAbstractBuilder().build( PolyDouble.class ) );
            case INTEGER:
                return PolySerializable.deserialize( json, getAbstractBuilder().build( PolyInteger.class ) );
        }
        throw new NotImplementedException();
    }


    public static String serialize( PolyValue value ) {
        switch ( value.type ) {
            case BOOLEAN:
                return PolySerializable.serialize( getAbstractBuilder().build( PolyBoolean.class ), (PolyBoolean) value );
            case VARCHAR:
                return PolySerializable.serialize( getAbstractBuilder().build( PolyString.class ), (PolyString) value );
            case DOCUMENT:
                return PolySerializable.serialize( getAbstractBuilder().build( PolyDocument.class ), (PolyDocument) value );
            case FLOAT:
                return PolySerializable.serialize( getAbstractBuilder().build( PolyFloat.class ), (PolyFloat) value );
            case DOUBLE:
                return PolySerializable.serialize( getAbstractBuilder().build( PolyDouble.class ), (PolyDouble) value );
            case INTEGER:
                return PolySerializable.serialize( getAbstractBuilder().build( PolyInteger.class ), (PolyInteger) value );
        }

        throw new NotImplementedException();
    }


    public boolean isSameType( PolyValue value ) {
        return type == value.type;
    }


    public boolean isNull() {
        return type == PolyType.NULL;
    }


    public PolyNull asNull() {
        return (PolyNull) this;
    }


    public boolean isBoolean() {
        return type == PolyType.BOOLEAN;
    }


    @Nullable
    public PolyBoolean asBoolean() {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        return null;
    }


    public boolean isInteger() {
        return type == PolyType.INTEGER;
    }


    @Nullable
    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }
        return null;
    }


    public boolean isDocument() {
        return type == PolyType.DOCUMENT;
    }


    @Nullable
    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        return null;
    }


    public boolean isList() {
        return type == PolyType.ARRAY;
    }


    @Nullable
    public <T extends PolyValue> PolyList<T> asList() {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        return null;
    }


    public boolean isString() {
        return type == PolyType.VARCHAR;
    }


    @Nullable
    public PolyString asString() {
        if ( isString() ) {
            return (PolyString) this;
        }
        return null;
    }


    public boolean isFloat() {
        return type == PolyType.FLOAT;
    }


    @Nullable
    public PolyFloat asFloat() {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }
        return null;
    }


    public boolean isDouble() {
        return type == PolyType.DOUBLE;
    }


    @Nullable
    public PolyDouble asDouble() {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }
        return null;
    }


    public boolean isLong() {
        return type == PolyType.BIGINT;
    }


    public PolyLong asLong() {
        if ( !isLong() ) {
            return null;
        }
        return (PolyLong) this;
    }


    public boolean isDate() {
        return type == PolyType.DATE;
    }


    public PolyDate asDate() {
        if ( !isDate() ) {
            return null;
        }
        return (PolyDate) this;
    }


    public boolean isTime() {
        return type == PolyType.TIME;
    }


    public PolyTime asTime() {
        if ( !isTime() ) {
            return null;
        }

        return (PolyTime) this;
    }


    public boolean isTimestamp() {
        return type == PolyType.TIMESTAMP;
    }


    public PolyTimeStamp asTimeStamp() {
        if ( !isTimestamp() ) {
            return null;
        }
        return (PolyTimeStamp) this;
    }


    public boolean isMap() {
        return type == PolyType.MAP;
    }


    public PolyMap<PolyValue, PolyValue> asMap() {
        if ( !isMap() ) {
            return null;
        }
        return (PolyMap<PolyValue, PolyValue>) this;
    }


    public boolean isEdge() {
        return type == PolyType.EDGE;
    }


    @Nullable
    public PolyEdge asEdge() {
        if ( !isEdge() ) {
            return null;
        }
        return (PolyEdge) this;
    }


    public boolean isNode() {
        return type == PolyType.NODE;
    }


    @Nullable
    public PolyNode asNode() {
        if ( !isNode() ) {
            return null;
        }
        return (PolyNode) this;
    }


    public boolean isPath() {
        return type == PolyType.PATH;
    }


    @Nullable
    public PolyPath asPath() {
        if ( !isPath() ) {
            return null;
        }
        return (PolyPath) this;
    }


    public boolean isGraph() {
        return type == PolyType.GRAPH;
    }


    @Nullable
    public PolyGraph asGraph() {
        if ( !isGraph() ) {
            return null;
        }
        return (PolyGraph) this;
    }


    public boolean isNumber() {
        return PolyType.NUMERIC_TYPES.contains( type );
    }


    public static class PolyValueSerializerDef extends SimpleSerializerDef<PolyValue> {

        @Override
        protected BinarySerializer<PolyValue> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyValue item ) {
                    out.writeUTF8( item.type.getTypeName() );
                    out.writeUTF8( PolyValue.serialize( item ) );
                }


                @Override
                public PolyValue decode( BinaryInput in ) throws CorruptedDataException {
                    return PolyValue.deserialize( PolyType.valueOf( in.readUTF8() ), in.readUTF8() );
                }
            };
        }

    }


}
