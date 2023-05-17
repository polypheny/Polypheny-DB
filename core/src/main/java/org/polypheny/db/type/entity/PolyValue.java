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
import java.lang.reflect.Type;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyDouble.PolyDoubleSerializerDef;
import org.polypheny.db.type.entity.PolyFloat.PolyFloatSerializerDef;
import org.polypheny.db.type.entity.PolyInteger.PolyIntegerSerializerDef;
import org.polypheny.db.type.entity.PolyString.PolyStringSerializerDef;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializerDef;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;

@Value
@Slf4j
@EqualsAndHashCode
@NonFinal
public abstract class PolyValue implements Expressible, Comparable<PolyValue>, PolySerializable {

    @NonFinal
    public static BinarySerializer<PolyValue> serializer = PolyValue.getAbstractBuilder().build( PolyValue.class );

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


    public static PolyValue getInitial( Type type ) {
        return PolyDefaults.DEFAULTS.get( type );
    }


    public static Type ofPrimitive( Type type ) {
        return PolyDefaults.PRIMITIVES.get( type );
    }


    @Override
    public <T extends PolySerializable> BinarySerializer<T> getSerializer() {
        return null;
    }


    public static Class<? extends PolyValue> classFrom( PolyType polyType ) {
        switch ( polyType ) {

            case BOOLEAN:
                return PolyBoolean.class;
            case TINYINT:
                return PolyInteger.class;
            case SMALLINT:
                return PolyInteger.class;
            case INTEGER:
                return PolyInteger.class;
            case BIGINT:
                return PolyBigDecimal.class;
            case DECIMAL:
                return PolyBigDecimal.class;
            case FLOAT:
                return PolyFloat.class;
            case REAL:
                return PolyFloat.class;
            case DOUBLE:
                return PolyDouble.class;
            case DATE:
                return PolyDate.class;
            case TIME:
                return PolyTime.class;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return PolyTime.class;
            case TIMESTAMP:
                return PolyTimeStamp.class;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PolyTimeStamp.class;
            case INTERVAL_YEAR:
                return PolyInterval.class;
            case INTERVAL_YEAR_MONTH:
                return PolyInterval.class;
            case INTERVAL_MONTH:
                return PolyInterval.class;
            case INTERVAL_DAY:
                return PolyInterval.class;
            case INTERVAL_DAY_HOUR:
                return PolyInterval.class;
            case INTERVAL_DAY_MINUTE:
                return PolyInterval.class;
            case INTERVAL_DAY_SECOND:
                return PolyInterval.class;
            case INTERVAL_HOUR:
                return PolyInterval.class;
            case INTERVAL_HOUR_MINUTE:
                return PolyInterval.class;
            case INTERVAL_HOUR_SECOND:
                return PolyInterval.class;
            case INTERVAL_MINUTE:
                return PolyInterval.class;
            case INTERVAL_MINUTE_SECOND:
                return PolyInterval.class;
            case INTERVAL_SECOND:
                return PolyInterval.class;
            case CHAR:
                return PolyString.class;
            case VARCHAR:
                return PolyString.class;
            case BINARY:
                return PolyBinary.class;
            case VARBINARY:
                return PolyBinary.class;
            case NULL:
                return PolyNull.class;
            case ANY:
                return PolyValue.class;
            case SYMBOL:
                return PolySymbol.class;
            case MULTISET:
                return PolyList.class;
            case ARRAY:
                return PolyList.class;
            case MAP:
                return PolyMap.class;
            case DOCUMENT:
                return PolyDocument.class;
            case GRAPH:
                return PolyGraph.class;
            case NODE:
                return PolyNode.class;
            case EDGE:
                return PolyEdge.class;
            case PATH:
                return PolyPath.class;
            case DISTINCT:
                return PolyValue.class;
            case STRUCTURED:
                return PolyValue.class;
            case ROW:
                return PolyList.class;
            case OTHER:
                return PolyValue.class;
            case CURSOR:
                return PolyValue.class;
            case COLUMN_LIST:
                return PolyList.class;
            case DYNAMIC_STAR:
                return PolyValue.class;
            case GEOMETRY:
                return PolyValue.class;
            case FILE:
                break;
            case IMAGE:
                break;
            case VIDEO:
                break;
            case AUDIO:
                break;
            case JSON:
                return PolyString.class;
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


    @NonNull
    public PolyBoolean asBoolean() {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isInteger() {
        return type == PolyType.INTEGER;
    }


    @NonNull
    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }
        if ( isNumber() ) {
            log.warn( "still not sure about this" );
            return PolyInteger.of( this.asNumber().intValue() );
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isDocument() {
        return type == PolyType.DOCUMENT;
    }


    @NonNull
    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isList() {
        return type == PolyType.ARRAY;
    }


    @NonNull
    public <T extends PolyValue> PolyList<T> asList() {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isString() {
        return type == PolyType.VARCHAR;
    }


    @NonNull
    public PolyString asString() {
        if ( isString() ) {
            return (PolyString) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isBinary() {
        return type == PolyType.BINARY;
    }


    @NonNull
    public PolyBinary asBinary() {
        if ( isBinary() ) {
            return (PolyBinary) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isBigDecimal() {
        return type == PolyType.DECIMAL;
    }


    @NonNull
    public PolyBigDecimal asBigDecimal() {
        if ( isBigDecimal() ) {
            return (PolyBigDecimal) this;
        }
        if ( isNumber() ) {
            log.warn( "still not sure about this" );
            return PolyBigDecimal.of( this.asNumber().bigDecimalValue() );
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isFloat() {
        return type == PolyType.FLOAT;
    }


    @NonNull
    public PolyFloat asFloat() {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }
        if ( isNumber() ) {
            log.warn( "still not sure about this" );
            return PolyFloat.of( (float) this.asNumber().doubleValue() );
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isDouble() {
        return type == PolyType.DOUBLE;
    }


    @NonNull
    public PolyDouble asDouble() {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }
        if ( isNumber() ) {
            log.warn( "still not sure about this" );
            return PolyDouble.of( this.asNumber().doubleValue() );
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isLong() {
        return type == PolyType.BIGINT;
    }


    @NonNull
    public PolyLong asLong() {
        if ( isLong() ) {
            return (PolyLong) this;
        }
        if ( isNumber() ) {
            log.warn( "still not sure about this" );
            return PolyLong.of( this.asNumber().longValue() );
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isTemporal() {
        return PolyType.DATETIME_TYPES.contains( type );
    }


    public PolyTemporal asTemporal() {
        if ( isTemporal() ) {
            return (PolyTemporal) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isDate() {
        return type == PolyType.DATE;
    }


    @NonNull
    public PolyDate asDate() {
        if ( isDate() ) {
            return (PolyDate) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isTime() {
        return type == PolyType.TIME;
    }


    @NonNull
    public PolyTime asTime() {
        if ( isTime() ) {
            return (PolyTime) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isTimestamp() {
        return type == PolyType.TIMESTAMP;
    }


    @NonNull
    public PolyTimeStamp asTimeStamp() {
        if ( isTimestamp() ) {
            return (PolyTimeStamp) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isMap() {
        return type == PolyType.MAP;
    }


    @NonNull
    public PolyMap<PolyValue, PolyValue> asMap() {
        if ( isMap() ) {
            return (PolyMap<PolyValue, PolyValue>) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isEdge() {
        return type == PolyType.EDGE;
    }


    @NonNull
    public PolyEdge asEdge() {
        if ( isEdge() ) {
            return (PolyEdge) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isNode() {
        return type == PolyType.NODE;
    }


    @NonNull
    public PolyNode asNode() {
        if ( isNode() ) {
            return (PolyNode) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isPath() {
        return type == PolyType.PATH;
    }


    @NonNull
    public PolyPath asPath() {
        if ( isPath() ) {
            return (PolyPath) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isGraph() {
        return type == PolyType.GRAPH;
    }


    @NonNull
    public PolyGraph asGraph() {
        if ( isGraph() ) {
            return (PolyGraph) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isNumber() {
        return PolyType.NUMERIC_TYPES.contains( type );
    }


    public PolyNumber asNumber() {
        if ( isNumber() ) {
            return (PolyNumber) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isInterval() {
        return PolyType.INTERVAL_TYPES.contains( type );
    }


    public PolyInterval asInterval() {
        if ( isInterval() ) {
            return (PolyInterval) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isSymbol() {
        return type == PolyType.SYMBOL;
    }


    public PolySymbol asSymbol() {
        if ( isSymbol() ) {
            return (PolySymbol) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isBlob() {
        return PolyType.BLOB_TYPES.contains( type );
    }


    public PolyBlob asBlob() {
        if ( isBlob() ) {
            return (PolyBlob) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public static PolyValue convert( PolyValue value, PolyType type ) {
        switch ( type ) {
            case INTEGER:
                return PolyInteger.from( value );
        }

        throw new GenericRuntimeException( String.format( "%s does not support conversion to %s.", value, type ) );
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
