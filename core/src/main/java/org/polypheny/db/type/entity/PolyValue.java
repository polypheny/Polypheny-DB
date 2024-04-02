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

package org.polypheny.db.type.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SerializerFactory;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.def.SimpleSerializerDef;
import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonDocument;
import org.bson.json.JsonParseException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary.ByteStringDeserializer;
import org.polypheny.db.type.entity.PolyBinary.ByteStringSerializer;
import org.polypheny.db.type.entity.PolyBoolean.PolyBooleanSerializerDef;
import org.polypheny.db.type.entity.PolyList.PolyListSerializerDef;
import org.polypheny.db.type.entity.PolyLong.PolyLongSerializerDef;
import org.polypheny.db.type.entity.PolyNull.PolyNullSerializerDef;
import org.polypheny.db.type.entity.PolyString.PolyStringSerializerDef;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyDocument.PolyDocumentSerializerDef;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyDictionary.PolyDictionarySerializerDef;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.PolyEdgeSerializerDef;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyGraph.PolyGraphSerializerDef;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyNode.PolyNodeSerializerDef;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal.PolyBigDecimalSerializerDef;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyDouble.PolyDoubleSerializerDef;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyFloat.PolyFloatSerializerDef;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyInteger.PolyIntegerSerializerDef;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.relational.PolyMap.PolyMapSerializerDef;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.BsonUtil;

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
        PolyTimestamp.class,
        PolyDocument.class,
        PolyDictionary.class,
        PolyDate.class,
        PolyMap.class,
        PolyList.class,
        PolyGraph.class,
        PolyBoolean.class,
        PolyTime.class,
        PolyString.class,
        PolyLong.class,
        PolyBinary.class,
        PolyNode.class,
        PolyEdge.class,
        PolyPath.class }) // add on Constructor already exists exception
@JsonTypeInfo(use = Id.NAME) // to allow typed json serialization
@JsonSubTypes({
        @JsonSubTypes.Type(value = PolyList.class, name = "LIST"),
        @JsonSubTypes.Type(value = PolyBigDecimal.class, name = "DECIMAL"),
        @JsonSubTypes.Type(value = PolyNull.class, name = "NULL"),
        @JsonSubTypes.Type(value = PolyString.class, name = "STRING"),
        @JsonSubTypes.Type(value = PolyDate.class, name = "DATE"),
        @JsonSubTypes.Type(value = PolyTime.class, name = "TIME"),
        @JsonSubTypes.Type(value = PolyDouble.class, name = "DOUBLE"),
        @JsonSubTypes.Type(value = PolyFloat.class, name = "FLOAT"),
        @JsonSubTypes.Type(value = PolyLong.class, name = "LONG"),
        @JsonSubTypes.Type(value = PolyInteger.class, name = "INTEGER"),
        @JsonSubTypes.Type(value = PolyBoolean.class, name = "BOOLEAN"),
        @JsonSubTypes.Type(value = PolyTimestamp.class, name = "TIMESTAMP"),
        @JsonSubTypes.Type(value = PolyBinary.class, name = "BINARY"),
        @JsonSubTypes.Type(value = PolyDocument.class, name = "DOCUMENT"),
        @JsonSubTypes.Type(value = PolySymbol.class, name = "SYMBOL"),
        @JsonSubTypes.Type(value = PolyMap.class, name = "MAP"),
        @JsonSubTypes.Type(value = PolyNode.class, name = "NODE"),
        @JsonSubTypes.Type(value = PolyEdge.class, name = "EDGE"),
        @JsonSubTypes.Type(value = PolyPath.class, name = "PATH"),
        @JsonSubTypes.Type(value = PolyDictionary.class, name = "DICTIONARY"),
        @JsonSubTypes.Type(value = PolyUserDefinedValue.class, name = "UDV")
})
public abstract class PolyValue implements Expressible, Comparable<PolyValue>, PolySerializable {

    @JsonIgnore
    // used internally to serialize into binary format
    public static BinarySerializer<PolyValue> serializer = SerializerFactory.builder()
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
            .with( PolyEdge.class, ctx -> new PolyEdgeSerializerDef() )
            .with( PolyNull.class, ctx -> new PolyNullSerializerDef() )
            .with( PolyBoolean.class, ctx -> new PolyBooleanSerializerDef() )
            .with( PolyGraph.class, ctx -> new PolyGraphSerializerDef() )
            .with( PolyLong.class, ctx -> new PolyLongSerializerDef() )
            .build().create( CLASS_LOADER, PolyValue.class );


    public static final ObjectMapper JSON_WRAPPER = JsonMapper.builder()
            .configure( MapperFeature.REQUIRE_TYPE_ID_FOR_SUBTYPES, true )
            .configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false )
            .configure( SerializationFeature.FAIL_ON_EMPTY_BEANS, false )
            .configure( MapperFeature.USE_STATIC_TYPING, true )
            .addModule( new SimpleModule()
                    .addSerializer( ByteString.class, new ByteStringSerializer() )
                    .addDeserializer( ByteString.class, new ByteStringDeserializer() ) )
            .build();


    static {
        JSON_WRAPPER.setSerializationInclusion( JsonInclude.Include.NON_NULL )
                .setVisibility( JSON_WRAPPER.getSerializationConfig().getDefaultVisibilityChecker()
                        .withIsGetterVisibility( Visibility.NONE )
                        .withGetterVisibility( Visibility.NONE )
                        .withSetterVisibility( Visibility.NONE ) )
                .writerWithDefaultPrettyPrinter();
    }


    @Serialize
    @JsonIgnore
    public PolyType type;


    @NonFinal
    @JsonIgnore
    Long byteSize;


    public PolyValue( PolyType type ) {
        this.type = type;
    }


    public static Function1<PolyValue, Object> getPolyToJava( AlgDataType type, boolean arrayAsList ) {
        return switch ( type.getPolyType() ) {
            case VARCHAR, CHAR, TEXT -> o -> o.asString().value;
            case INTEGER, TINYINT, SMALLINT -> o -> o.asNumber().IntValue();
            case FLOAT, REAL -> o -> o.asNumber().FloatValue();
            case DOUBLE -> o -> o.asNumber().DoubleValue();
            case BIGINT -> o -> o.asNumber().LongValue();
            case DECIMAL -> o -> o.asNumber().BigDecimalValue();
            case DATE -> o -> o.asDate().getDaysSinceEpoch();
            case TIME -> o -> o.asTime().getMillisOfDay();
            case TIMESTAMP -> o -> o.asTimestamp().millisSinceEpoch;
            case BOOLEAN -> o -> o.asBoolean().value;
            case ARRAY -> {
                Function1<PolyValue, Object> elTrans = getPolyToJava( getAndDecreaseArrayDimensionIfNecessary( (ArrayType) type ), arrayAsList );
                yield o -> o == null || o.isNull()
                        ? null
                        : arrayAsList
                                ? (o.asList().value.stream().map( elTrans::apply ).toList())
                                : o.asList().value.stream().map( elTrans::apply ).toList().toArray();
            }
            case FILE, IMAGE, AUDIO, VIDEO -> o -> o.asBlob().asByteArray();
            case DOCUMENT -> o -> o.asDocument().toJson();
            default -> throw new NotImplementedException( "meta: " + type.getFullTypeString() );
        };
    }


    private static AlgDataType getAndDecreaseArrayDimensionIfNecessary( ArrayType type ) {
        AlgDataType component = type.getComponentType();
        while ( component.getPolyType() == PolyType.ARRAY ) {
            component = component.getComponentType();
        }

        if ( type.getDimension() > 1 ) {
            // we make the component the parent for the next step
            return AlgDataTypeFactory.DEFAULT.createArrayType( component, type.getMaxCardinality(), type.getDimension() - 1 );
        }
        return type.getComponentType();
    }


    public static Function1<PolyValue, Object> wrapNullableIfNecessary( Function1<PolyValue, Object> polyToExternalizer, boolean nullable ) {
        return nullable ? o -> o == null || o.isNull() ? null : polyToExternalizer.apply( o ) : polyToExternalizer;
    }


    public static PolyValue fromJson( String json ) {
        if ( json == null ) {
            return null;
        } else if ( json.trim().startsWith( "{" ) ) {
            return BsonUtil.toPolyValue( BsonDocument.parse( json ) );
        }

        try {
            return BsonUtil.toPolyValue( BsonDocument.parse( "{\"key\":" + json + "}" ) ).asMap().get( PolyString.of( "key" ) );
        } catch ( Throwable e ) {
            log.warn( "Error on deserializing JSON." );

            if ( e instanceof JsonParseException && e.getMessage().startsWith( "JSON reader was expecting a value but found '" ) ) {
                return PolyString.of( json );
            }
            throw new GenericRuntimeException( e );
        }

    }


    @NotNull
    public String toTypedJson() {
        try {
            return JSON_WRAPPER.writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing typed JSON." );
            return PolyNull.NULL.toTypedJson();
        }
    }


    @Nullable
    public static <E extends PolyValue> E fromTypedJson( String value, Class<E> clazz ) {
        try {
            return JSON_WRAPPER.readValue( value, clazz );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on deserialize typed JSON." );
            return null;
        }
    }


    public String toJson() {
        // fallback serializer
        try {
            return JSON_WRAPPER.writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on deserialize JSON." );
            return null;
        }
    }


    // used by code generation
    @SuppressWarnings("unused")
    public PolyString toPolyJson() {
        return PolyString.of( toJson() );
    }


    @NotNull
    public Optional<Long> getByteSize() {
        if ( byteSize == null ) {
            byteSize = deriveByteSize();
        }
        return Optional.ofNullable( byteSize );
    }


    @NotNull
    protected static String getConvertError( @NotNull Object object, Class<? extends PolyValue> clazz ) {
        return "Could not convert " + object + " to " + clazz.getSimpleName();
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
        return switch ( polyType ) {
            case BOOLEAN -> PolyBoolean.class;
            case TINYINT, INTEGER, SMALLINT -> PolyInteger.class;
            case BIGINT -> PolyLong.class;
            case DECIMAL -> PolyBigDecimal.class;
            case FLOAT, REAL -> PolyFloat.class;
            case DOUBLE -> PolyDouble.class;
            case DATE -> PolyDate.class;
            case TIME -> PolyTime.class;
            case TIMESTAMP -> PolyTimestamp.class;
            case INTERVAL -> PolyInterval.class;
            case CHAR -> PolyString.class;
            case VARCHAR -> PolyString.class;
            case BINARY -> PolyBinary.class;
            case VARBINARY -> PolyBinary.class;
            case NULL -> PolyNull.class;
            case ANY -> PolyValue.class;
            case SYMBOL -> PolySymbol.class;
            case MULTISET -> PolyList.class;
            case ARRAY -> PolyList.class;
            case MAP -> PolyMap.class;
            case DOCUMENT -> PolyDocument.class;
            case GRAPH -> PolyGraph.class;
            case NODE -> PolyNode.class;
            case EDGE -> PolyEdge.class;
            case PATH -> PolyPath.class;
            case DISTINCT -> PolyValue.class;
            case STRUCTURED -> PolyValue.class;
            case ROW -> PolyList.class;
            case OTHER -> PolyValue.class;
            case CURSOR -> PolyValue.class;
            case COLUMN_LIST -> PolyList.class;
            case DYNAMIC_STAR -> PolyValue.class;
            case GEOMETRY -> PolyValue.class;
            case FILE, IMAGE, VIDEO, AUDIO -> PolyBlob.class;
            case JSON -> PolyString.class;
            case TEXT -> PolyString.class;
            default -> throw new NotImplementedException( "value" );
        };
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


    public boolean isNotNull() {
        return !isNull();
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
        throw cannotParse( this, PolyBoolean.class );
    }


    @NotNull
    public GenericRuntimeException cannotParse( PolyValue value, Class<?> clazz ) {
        return new GenericRuntimeException( "Cannot parse %s to type %s", value, clazz.getSimpleName() );
    }


    public boolean isInteger() {
        return type == PolyType.INTEGER;
    }


    @NotNull
    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }

        throw cannotParse( this, PolyInteger.class );
    }


    public boolean isDocument() {
        return type == PolyType.DOCUMENT;
    }


    @NotNull
    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw cannotParse( this, PolyDocument.class );
    }


    public boolean isList() {
        return type == PolyType.ARRAY;
    }


    @NotNull
    public <T extends PolyValue> PolyList<T> asList() {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        throw cannotParse( this, PolyList.class );
    }


    public boolean isString() {
        return type == PolyType.VARCHAR;
    }


    @NotNull
    public PolyString asString() {
        if ( isString() ) {
            return (PolyString) this;
        }
        throw cannotParse( this, PolyString.class );
    }


    public boolean isText() {
        return type == PolyType.TEXT;
    }


    public boolean isBinary() {
        return type == PolyType.BINARY;
    }


    @NotNull
    public PolyBinary asBinary() {
        if ( isBinary() ) {
            return (PolyBinary) this;
        }
        throw cannotParse( this, PolyBinary.class );
    }


    public boolean isBigDecimal() {
        return type == PolyType.DECIMAL;
    }


    @NotNull
    public PolyBigDecimal asBigDecimal() {
        if ( isBigDecimal() ) {
            return (PolyBigDecimal) this;
        }

        throw cannotParse( this, PolyBigDecimal.class );
    }


    public boolean isFloat() {
        return type == PolyType.FLOAT;
    }


    @NotNull
    public PolyFloat asFloat() {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }

        throw cannotParse( this, PolyFloat.class );
    }


    public boolean isDouble() {
        return type == PolyType.DOUBLE;
    }


    @NotNull
    public PolyDouble asDouble() {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }

        throw cannotParse( this, PolyDouble.class );
    }


    public boolean isLong() {
        return type == PolyType.BIGINT;
    }


    @NotNull
    public PolyLong asLong() {
        if ( isLong() ) {
            return (PolyLong) this;
        }

        throw cannotParse( this, PolyLong.class );
    }


    public boolean isTemporal() {
        return PolyType.DATETIME_TYPES.contains( type );
    }


    public PolyTemporal asTemporal() {
        if ( isTemporal() ) {
            return (PolyTemporal) this;
        }
        throw cannotParse( this, PolyTemporal.class );
    }


    public boolean isDate() {
        return type == PolyType.DATE;
    }


    @NotNull
    public PolyDate asDate() {
        if ( isDate() ) {
            return (PolyDate) this;
        }
        throw cannotParse( this, PolyDate.class );
    }


    public boolean isTime() {
        return type == PolyType.TIME;
    }


    @NotNull
    public PolyTime asTime() {
        if ( isTime() ) {
            return (PolyTime) this;
        }

        throw cannotParse( this, PolyTime.class );
    }


    public boolean isTimestamp() {
        return type == PolyType.TIMESTAMP;
    }


    @NotNull
    public PolyTimestamp asTimestamp() {
        if ( isTimestamp() ) {
            return (PolyTimestamp) this;
        }

        throw cannotParse( this, PolyTimestamp.class );
    }


    public boolean isMap() {
        return type == PolyType.MAP;
    }


    @NotNull
    public PolyMap<PolyValue, PolyValue> asMap() {
        if ( isMap() || isDocument() ) {
            return (PolyMap<PolyValue, PolyValue>) this;
        }
        throw cannotParse( this, PolyMap.class );
    }


    public boolean isEdge() {
        return type == PolyType.EDGE;
    }


    @NotNull
    public PolyEdge asEdge() {
        if ( isEdge() ) {
            return (PolyEdge) this;
        }
        throw cannotParse( this, PolyEdge.class );
    }


    public boolean isNode() {
        return type == PolyType.NODE;
    }


    @NotNull
    public PolyNode asNode() {
        if ( isNode() ) {
            return (PolyNode) this;
        }
        throw cannotParse( this, PolyNode.class );
    }


    public boolean isPath() {
        return type == PolyType.PATH;
    }


    @NotNull
    public PolyPath asPath() {
        if ( isPath() ) {
            return (PolyPath) this;
        }
        throw cannotParse( this, PolyPath.class );
    }


    public boolean isGraph() {
        return type == PolyType.GRAPH;
    }


    @NotNull
    public PolyGraph asGraph() {
        if ( isGraph() ) {
            return (PolyGraph) this;
        }
        throw cannotParse( this, PolyGraph.class );
    }


    public boolean isNumber() {
        return PolyType.NUMERIC_TYPES.contains( type );
    }


    @NotNull
    public PolyNumber asNumber() {
        if ( isNumber() ) {
            return (PolyNumber) this;
        }
        throw cannotParse( this, PolyNumber.class );
    }


    public boolean isInterval() {
        return PolyType.INTERVAL_TYPES.contains( type );
    }


    @NotNull
    public PolyInterval asInterval() {
        if ( isInterval() ) {
            return (PolyInterval) this;
        }
        throw cannotParse( this, PolyInterval.class );
    }


    public boolean isSymbol() {
        return type == PolyType.SYMBOL;
    }


    public PolySymbol asSymbol() {
        if ( isSymbol() ) {
            return (PolySymbol) this;
        }
        throw cannotParse( this, PolySymbol.class );
    }


    public boolean isBlob() {
        return PolyType.BLOB_TYPES.contains( type );
    }


    @NotNull
    public PolyBlob asBlob() {
        if ( isBlob() ) {
            return (PolyBlob) this;
        }
        throw cannotParse( this, PolyBlob.class );
    }


    public boolean isUserDefinedValue() {
        return PolyType.USER_DEFINED_TYPE == type;
    }


    @NotNull
    public PolyUserDefinedValue asUserDefinedValue() {
        if ( isUserDefinedValue() ) {
            return (PolyUserDefinedValue) this;
        }
        throw cannotParse( this, PolyUserDefinedValue.class );
    }


    public static PolyValue convert( PolyValue value, PolyType type ) {

        switch ( type ) {
            case INTEGER:
                return PolyInteger.convert( value );
            case DOCUMENT:
                // docs accept all
                return value;
            case BIGINT:
                return PolyLong.convert( value );
        }
        if ( type.getFamily() == value.getType().getFamily() ) {
            return value;
        }

        throw new GenericRuntimeException( "%s does not support conversion to %s.", value, type );
    }


    public static PolyValue fromType( Object object, PolyType type ) {
        return switch ( type ) {
            case BOOLEAN -> PolyBoolean.of( (Boolean) object );
            case TINYINT, SMALLINT, INTEGER -> PolyInteger.of( (Number) object );
            case BIGINT -> PolyLong.of( (Number) object );
            case DECIMAL -> PolyBigDecimal.of( object.toString() );
            case FLOAT, REAL -> PolyFloat.of( (Number) object );
            case DOUBLE -> PolyDouble.of( (Number) object );
            case DATE -> {
                if ( object instanceof Number number ) {
                    yield PolyDate.of( number );
                }
                if ( object instanceof Calendar calendar ) {
                    yield PolyDate.of( calendar.getTimeInMillis() );
                }
                throw new NotImplementedException();
            }
            case TIME -> {
                if ( object instanceof Number number ) {
                    yield PolyTime.of( number );
                } else if ( object instanceof Calendar calendar ) {
                    yield PolyTime.of( calendar.getTimeInMillis() );
                }
                throw new NotImplementedException();
            }
            case TIMESTAMP -> {
                if ( object instanceof Timestamp timestamp ) {
                    yield PolyTimestamp.of( timestamp );
                } else if ( object instanceof Calendar calendar ) {
                    yield PolyTimestamp.of( calendar.getTimeInMillis() );
                }
                throw new NotImplementedException();
            }
            case CHAR, VARCHAR -> PolyString.of( (String) object );
            case BINARY, VARBINARY -> {
                if ( object instanceof byte[] bytes ) {
                    yield PolyBinary.of( bytes );
                }
                yield PolyBinary.of( (ByteString) object );
            }
            default -> throw new NotImplementedException();
        };
    }


    public abstract Object toJava();


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
                    return PolySerializable.deserialize( in.readUTF8(), PolySerializable.buildSerializer( PolyValue.classFrom( type ) ) );
                }
            };
        }

    }

}
