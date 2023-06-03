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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.lang.reflect.Type;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBigDecimal.PolyBigDecimalSerializerDef;
import org.polypheny.db.type.entity.PolyBigDecimal.PolyBigDecimalTypeAdapter;
import org.polypheny.db.type.entity.PolyBinary.PolyBinaryTypeAdapter;
import org.polypheny.db.type.entity.PolyBoolean.PolyBooleanTypeAdapter;
import org.polypheny.db.type.entity.PolyDate.PolyDateTypeAdapter;
import org.polypheny.db.type.entity.PolyDouble.PolyDoubleSerializerDef;
import org.polypheny.db.type.entity.PolyDouble.PolyDoubleTypeAdapter;
import org.polypheny.db.type.entity.PolyFloat.PolyFloatSerializerDef;
import org.polypheny.db.type.entity.PolyFloat.PolyFloatTypeAdapter;
import org.polypheny.db.type.entity.PolyInteger.PolyIntegerSerializerDef;
import org.polypheny.db.type.entity.PolyInteger.PolyIntegerTypeAdapter;
import org.polypheny.db.type.entity.PolyList.PolyListSerializerDef;
import org.polypheny.db.type.entity.PolyList.PolyListTypeAdapter;
import org.polypheny.db.type.entity.PolyLong.PolyLongTypeAdapter;
import org.polypheny.db.type.entity.PolyString.PolyStringSerializerDef;
import org.polypheny.db.type.entity.PolyString.PolyStringTypeAdapter;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializerDef;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyNode.PolyNodeSerializerDef;
import org.polypheny.db.type.entity.graph.PolyNode.PolyNodeTypeAdapter;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.relational.PolyMap.PolyMapSerializerDef;

@Value
@Slf4j
@EqualsAndHashCode
@NonFinal
@SerializeClass(subclasses = {
        PolyNull.class,
        PolyInteger.class,
        PolyFloat.class,
        PolyDouble.class,
        PolyBigDecimal.class,
        PolyTimeStamp.class,
        PolyDocument.class,
        PolyMap.class,
        PolyList.class,
        PolyBoolean.class,
        PolyTime.class,
        PolyString.class,
        PolyLong.class,
        PolyBinary.class,
        PolyNode.class,
        PolyEdge.class,
        PolyPath.class }) // add on Constructor already exists exception
public abstract class PolyValue implements Expressible, Comparable<PolyValue>, PolySerializable {

    public static BinarySerializer<PolyValue> serializer = PolySerializable.builder.get()
            .with( PolyInteger.class, ctx -> new PolyIntegerSerializerDef() )
            .with( PolyValue.class, ctx -> new PolyValueSerializerDef() )
            .with( PolyString.class, ctx -> new PolyStringSerializerDef() )
            .with( PolyFloat.class, ctx -> new PolyFloatSerializerDef() )
            .with( PolyDouble.class, ctx -> new PolyDoubleSerializerDef() )
            .with( PolyMap.class, ctx -> new PolyMapSerializerDef() )
            .with( PolyDocument.class, ctx -> new PolyDocumentSerializerDef() )
            .with( PolyDictionary.class, ctx -> new PolyDocumentSerializerDef() )
            .with( PolyList.class, ctx -> new PolyListSerializerDef() )
            .with( PolyBigDecimal.class, ctx -> new PolyBigDecimalSerializerDef() )
            .with( PolyNode.class, ctx -> new PolyNodeSerializerDef() )
            .build( PolyValue.class );

    public static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .registerTypeAdapter( PolyBigDecimal.class, new PolyBigDecimalTypeAdapter() )
            .registerTypeAdapter( PolyBinary.class, new PolyBinaryTypeAdapter() )
            .registerTypeAdapter( PolyBoolean.class, new PolyBooleanTypeAdapter() )
            .registerTypeAdapter( PolyDate.class, new PolyDateTypeAdapter() )
            .registerTypeAdapter( PolyDouble.class, new PolyDoubleTypeAdapter() )
            .registerTypeAdapter( PolyFloat.class, new PolyFloatTypeAdapter() )
            .registerTypeAdapter( PolyInteger.class, new PolyIntegerTypeAdapter() )
            .registerTypeAdapter( PolyList.class, new PolyListTypeAdapter<>() )
            .registerTypeAdapter( PolyLong.class, new PolyLongTypeAdapter() )
            .registerTypeAdapter( PolyString.class, new PolyStringTypeAdapter() )
            .registerTypeAdapter( PolyNode.class, new PolyNodeTypeAdapter() )
            .create();


    @Serialize
    public PolyType type;


    public PolyValue(
            @Deserialize("type") PolyType type ) {
        this.type = type;
    }


    public static Expression getInitialExpression( Type type ) {
        if ( PolyDefaults.DEFAULTS.get( type ) != null ) {
            return PolyDefaults.DEFAULTS.get( type ).asExpression();
        }

        return Expressions.constant( null, type );

    }


    public static PolyValue getInitial( Type type ) {
        return PolyDefaults.DEFAULTS.get( type );
    }


    public static Type ofPrimitive( Type input, PolyType polyType ) {
        Type type = PolyDefaults.PRIMITIVES.get( input );

        if ( type != null ) {
            return type;
        }

        return PolyDefaults.PRIMITIVES.get( PolyValue.classFrom( polyType ) );
    }


    public static PolyValue getNull( Class<?> clazz ) {
        return PolyDefaults.NULLS.get( clazz );
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
        return PolySerializable.deserialize( json, serializer );
    }


    @Override
    public String serialize() {
        return PolySerializable.serialize( serializer, this );
    }


    @Override
    public <T extends PolySerializable> BinarySerializer<T> getSerializer() {
        return (BinarySerializer<T>) serializer;
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
        if ( isMap() || isDocument() ) {
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
            case DOCUMENT:
                // docs accept all
                return value;
        }

        throw new GenericRuntimeException( String.format( "%s does not support conversion to %s.", value, type ) );
    }


    public String toJson() {
        return toString();
    }


    public static class PolyValueSerializerDef extends SimpleSerializerDef<PolyValue> {

        @Override
        protected BinarySerializer<PolyValue> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyValue item ) {
                    out.writeUTF8( item.type.getTypeName() );
                    out.writeUTF8( item.serialize() );
                }


                @Override
                public PolyValue decode( BinaryInput in ) throws CorruptedDataException {
                    PolyType type = PolyType.valueOf( in.readUTF8() );
                    return PolySerializable.deserialize( in.readUTF8(), PolySerializable.builder.get().build( PolyValue.classFrom( type ) ) );
                }
            };
        }

    }


}
