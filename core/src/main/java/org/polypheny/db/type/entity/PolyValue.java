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
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBigDecimal.PolyBigDecimalSerializer;
import org.polypheny.db.type.entity.PolyBigDecimal.PolyBigDecimalSerializerDef;
import org.polypheny.db.type.entity.PolyBinary.PolyBinarySerializer;
import org.polypheny.db.type.entity.PolyBoolean.PolyBooleanSerializer;
import org.polypheny.db.type.entity.PolyBoolean.PolyBooleanSerializerDef;
import org.polypheny.db.type.entity.PolyDate.PolyDateSerializer;
import org.polypheny.db.type.entity.PolyDouble.PolyDoubleSerializer;
import org.polypheny.db.type.entity.PolyDouble.PolyDoubleSerializerDef;
import org.polypheny.db.type.entity.PolyFloat.PolyFloatSerializer;
import org.polypheny.db.type.entity.PolyFloat.PolyFloatSerializerDef;
import org.polypheny.db.type.entity.PolyInteger.PolyIntegerSerializer;
import org.polypheny.db.type.entity.PolyInteger.PolyIntegerSerializerDef;
import org.polypheny.db.type.entity.PolyInterval.PolyIntervalSerializer;
import org.polypheny.db.type.entity.PolyList.PolyListSerializer;
import org.polypheny.db.type.entity.PolyList.PolyListSerializerDef;
import org.polypheny.db.type.entity.PolyLong.PolyLongSerializer;
import org.polypheny.db.type.entity.PolyNull.PolyNullSerializer;
import org.polypheny.db.type.entity.PolyNull.PolyNullSerializerDef;
import org.polypheny.db.type.entity.PolyString.PolyStringSerializer;
import org.polypheny.db.type.entity.PolyString.PolyStringSerializerDef;
import org.polypheny.db.type.entity.PolySymbol.PolySymbolSerializer;
import org.polypheny.db.type.entity.PolyTime.PolyTimeSerializer;
import org.polypheny.db.type.entity.PolyTimeStamp.PolyTimeStampSerializer;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializer;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializerDef;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyDictionary.PolyDictionarySerializer;
import org.polypheny.db.type.entity.graph.PolyDictionary.PolyDictionarySerializerDef;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.PolyEdgeSerializer;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyNode.PolyNodeSerializer;
import org.polypheny.db.type.entity.graph.PolyNode.PolyNodeSerializerDef;
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

    // used internally to serialize into binary format
    public static BinarySerializer<PolyValue> serializer = PolySerializable.builder.get()
            .with( PolyInteger.class, ctx -> new PolyIntegerSerializerDef() )
            .with( PolyValue.class, ctx -> new PolyValueSerializerDef() )
            .with( PolyString.class, ctx -> new PolyStringSerializerDef() )
            .with( PolyFloat.class, ctx -> new PolyFloatSerializerDef() )
            .with( PolyDouble.class, ctx -> new PolyDoubleSerializerDef() )
            .with( PolyMap.class, ctx -> new PolyMapSerializerDef() )
            .with( PolyDocument.class, ctx -> new PolyDocumentSerializerDef() )
            .with( PolyDictionary.class, ctx -> new PolyDictionarySerializerDef() )
            .with( PolyList.class, ctx -> new PolyListSerializerDef() )
            .with( PolyBigDecimal.class, ctx -> new PolyBigDecimalSerializerDef() )
            .with( PolyNode.class, ctx -> new PolyNodeSerializerDef() )
            .with( PolyNull.class, ctx -> new PolyNullSerializerDef() )
            .with( PolyBoolean.class, ctx -> new PolyBooleanSerializerDef() )
            .build( PolyValue.class );

    // used to serialize to Json
    public static final GsonBuilder GSON_BUILDER = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .registerTypeAdapter( PolyNode.class, new PolyNodeSerializer() )
            .registerTypeAdapter( PolyEdge.class, new PolyEdgeSerializer() )
            .registerTypeAdapter( PolyDictionary.class, new PolyDictionarySerializer() )
            .registerTypeAdapter( PolyDocument.class, new PolyDocumentSerializer() )
            .registerTypeAdapter( PolyBigDecimal.class, new PolyBigDecimalSerializer() )
            .registerTypeAdapter( PolyList.class, new PolyListSerializer<>() )
            .registerTypeAdapter( PolyString.class, new PolyStringSerializer() )
            .registerTypeAdapter( PolyLong.class, new PolyLongSerializer() )
            .registerTypeAdapter( PolyInteger.class, new PolyIntegerSerializer() )
            .registerTypeAdapter( PolyBoolean.class, new PolyBooleanSerializer() )
            .registerTypeAdapter( PolyDouble.class, new PolyDoubleSerializer() )
            .registerTypeAdapter( PolyBinary.class, new PolyBinarySerializer() )
            .registerTypeAdapter( PolyFloat.class, new PolyFloatSerializer() )
            .registerTypeAdapter( PolySymbol.class, new PolySymbolSerializer() )
            .registerTypeAdapter( PolyTime.class, new PolyTimeSerializer() )
            .registerTypeAdapter( PolyDate.class, new PolyDateSerializer() )
            .registerTypeAdapter( PolyTimeStamp.class, new PolyTimeStampSerializer() )
            .registerTypeAdapter( PolyInterval.class, new PolyIntervalSerializer() )
            .registerTypeAdapter( PolyNull.class, new PolyNullSerializer() )
            .registerTypeAdapter( PolyValue.class, new PolyValueTypeAdapter() );


    public static final Gson GSON = GSON_BUILDER.create();


    @Serialize
    public PolyType type;


    @NonFinal
    Long byteSize;


    public PolyValue(
            @Deserialize("type") PolyType type ) {
        this.type = type;
    }


    @NotNull
    public Optional<Long> getByteSize() {
        if ( byteSize == null ) {
            byteSize = deriveByteSize();
        }
        return Optional.ofNullable( byteSize );
    }


    @Nullable
    public abstract Long deriveByteSize();


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
                return PolyLong.class;
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
                return PolyFile.class;
            case IMAGE:
                break;
            case VIDEO:
                break;
            case AUDIO:
                break;
            case JSON:
                return PolyString.class;
        }
        throw new NotImplementedException( "value" );
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


    @NotNull
    public PolyBoolean asBoolean() {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isInteger() {
        return type == PolyType.INTEGER;
    }


    @NotNull
    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isDocument() {
        return type == PolyType.DOCUMENT;
    }


    @NotNull
    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isList() {
        return type == PolyType.ARRAY;
    }


    @NotNull
    public <T extends PolyValue> PolyList<T> asList() {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isString() {
        return type == PolyType.VARCHAR;
    }


    @NotNull
    public PolyString asString() {
        if ( isString() ) {
            return (PolyString) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isBinary() {
        return type == PolyType.BINARY;
    }


    @NotNull
    public PolyBinary asBinary() {
        if ( isBinary() ) {
            return (PolyBinary) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isBigDecimal() {
        return type == PolyType.DECIMAL;
    }


    @NotNull
    public PolyBigDecimal asBigDecimal() {
        if ( isBigDecimal() ) {
            return (PolyBigDecimal) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isFloat() {
        return type == PolyType.FLOAT;
    }


    @NotNull
    public PolyFloat asFloat() {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isDouble() {
        return type == PolyType.DOUBLE;
    }


    @NotNull
    public PolyDouble asDouble() {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isLong() {
        return type == PolyType.BIGINT;
    }


    @NotNull
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


    @NotNull
    public PolyDate asDate() {
        if ( isDate() ) {
            return (PolyDate) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isTime() {
        return type == PolyType.TIME;
    }


    @NotNull
    public PolyTime asTime() {
        if ( isTime() ) {
            return (PolyTime) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isTimestamp() {
        return type == PolyType.TIMESTAMP;
    }


    @NotNull
    public PolyTimeStamp asTimeStamp() {
        if ( isTimestamp() ) {
            return (PolyTimeStamp) this;
        }

        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isMap() {
        return type == PolyType.MAP;
    }


    @NotNull
    public PolyMap<PolyValue, PolyValue> asMap() {
        if ( isMap() || isDocument() ) {
            return (PolyMap<PolyValue, PolyValue>) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isEdge() {
        return type == PolyType.EDGE;
    }


    @NotNull
    public PolyEdge asEdge() {
        if ( isEdge() ) {
            return (PolyEdge) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isNode() {
        return type == PolyType.NODE;
    }


    @NotNull
    public PolyNode asNode() {
        if ( isNode() ) {
            return (PolyNode) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isPath() {
        return type == PolyType.PATH;
    }


    @NotNull
    public PolyPath asPath() {
        if ( isPath() ) {
            return (PolyPath) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isGraph() {
        return type == PolyType.GRAPH;
    }


    @NotNull
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


    @NotNull
    public PolyBlob asBlob() {
        if ( isBlob() ) {
            return (PolyBlob) this;
        }
        throw new GenericRuntimeException( "Cannot parse " + this );
    }


    public boolean isUserDefinedValue() {
        return PolyType.USER_DEFINED_TYPE == type;
    }


    public PolyUserDefinedValue asUserDefinedValue() {
        if ( isUserDefinedValue() ) {
            return (PolyUserDefinedValue) this;
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
        if ( type.getFamily() == value.getType().getFamily() ) {
            return value;
        }

        throw new GenericRuntimeException( "%s does not support conversion to %s.", value, type );
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
                    out.writeUTF16( item.serialize() );
                }


                @Override
                public PolyValue decode( BinaryInput in ) throws CorruptedDataException {
                    PolyType type = PolyType.valueOf( in.readUTF8() );
                    return PolySerializable.deserialize( in.readUTF16(), PolySerializable.builder.get().build( PolyValue.classFrom( type ) ) );
                }
            };
        }

    }


    public static class PolyValueTypeAdapter extends TypeAdapter<PolyValue> {

        private static final String CLASSNAME = "className";
        private static final String INSTANCE = "instance";


        @Override
        public void write( JsonWriter out, PolyValue value ) throws IOException {
            if ( value == null ) {
                out.nullValue();
                return;
            }

            out.beginObject();
            out.name( CLASSNAME );
            out.value( value.getClass().getName() );
            out.name( INSTANCE );
            GSON.toJson( value, value.getClass(), out );
            out.endObject();
        }


        @Override
        public PolyValue read( JsonReader in ) throws IOException {
            JsonToken token = in.peek();
            if ( token == JsonToken.NULL ) {
                in.nextNull();
                return null;
            }

            in.beginObject();
            String className = null;
            PolyValue instance = null;

            while ( in.hasNext() ) {
                String name = in.nextName();
                if ( name.equals( CLASSNAME ) ) {
                    className = in.nextString();
                } else if ( name.equals( INSTANCE ) ) {
                    Type type;
                    try {
                        type = Class.forName( className );
                    } catch ( ClassNotFoundException e ) {
                        throw new JsonParseException( "Invalid class name: " + className, e );
                    }
                    instance = GSON.fromJson( in, type );
                } else {
                    in.skipValue();
                }
            }

            in.endObject();
            return instance;
        }

    }

}
