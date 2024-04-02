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

package org.polypheny.db.util;

import com.mongodb.client.gridfs.GridFSBucket;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.ComparableList;
import org.polypheny.db.runtime.PolyCollections.FlatMap;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;


public class BsonUtil {

    private final static List<Pair<String, String>> mappings = new ArrayList<>();
    private final static List<String> stops = new ArrayList<>();


    static {
        mappings.add( new Pair<>( "(?<=[a-zA-Z0-9)\"][ ]{0,10})\\-", "\"$subtract\"" ) );
        mappings.add( new Pair<>( "\\+", "\"$add\"" ) );
        mappings.add( new Pair<>( "\\*", "\"$multiply\"" ) );
        mappings.add( new Pair<>( "\\/", "\"$divide\"" ) );

        stops.add( "," );
        stops.add( "}" );
    }


    /**
     * operations which include /*+_ cannot be parsed by the bsonDocument parser,
     * so they need to be replaced by an equivalent bson compatible operation
     * 1-3*10 {@code ->} {$subtract: [1, {$multiply:[3,10]}]}
     *
     * @param bson the full bson string
     * @return the initial bson string with the exchanged calculation
     */
    public static String fixBson( String bson ) {
        bson = bson.replace( "/", "\\/" );
        String reg = "([a-zA-Z0-9$._]|[\"]{2})+(\\s*[*/+-]\\s*[-]*\\s*([a-zA-Z0-9$._]|[\"]{2})+)+";

        if ( bson.split( reg ).length == 1 ) {
            return bson;
        }

        Pattern p = Pattern.compile( reg );
        Matcher m = p.matcher( bson );

        while ( m.find() ) {
            int count = 0;
            for ( int i = 0; i <= m.start(); i++ ) {
                char temp = bson.charAt( i );
                if ( temp == '"' ) {
                    count++;
                }
            }
            if ( count % 2 != 0 ) {
                continue;
            }
            String match = m.group( 0 );
            String calculation = fixCalculation( match, 0 );
            bson = bson.replace( match, calculation );
        }

        return bson;
    }


    /**
     * Recursively iterates over the operation mappings and replaces them in them with the equivalent BSON compatible format
     *
     * "{"key": 3*10-18}" {@code ->} "{"key": {"subtract":[18, {"multiply": [3, 10]}]}}"
     *
     * @param calculation the calculation up to this point
     * @param depth how many operations of [+-/*] where already replaced
     * @return a completely replaced representation
     */
    private static String fixCalculation( String calculation, int depth ) {
        if ( depth > mappings.size() - 1 ) {
            return calculation;
        }

        Pair<String, String> entry = mappings.get( depth );
        List<String> splits = Arrays.asList( calculation.split( entry.getKey() ) );

        if ( splits.size() > 1 ) {
            List<String> parts = splits.stream().map( s -> fixCalculation( s, depth + 1 ) ).collect( Collectors.toList() );
            return "{" + entry.getValue() + " : [" + String.join( ",", parts ) + "]}";
        } else {
            return fixCalculation( calculation, depth + 1 );
        }
    }


    /**
     * Cast the collection of document values to document
     */
    public static List<BsonDocument> asDocumentCollection( List<BsonValue> values ) {
        return values.stream().map( BsonValue::asDocument ).collect( Collectors.toList() );
    }


    public static String getObjectId() {
        return new ObjectId().toHexString();
    }


    /**
     * Direct transformation of an untyped input to the correct Bson format according to the
     * provided PolyType.
     *
     * @param obj the input
     * @param type the corresponding PolyType
     * @param bucket the bucket to retrieve distributed multimedia objects
     * @return the object in the corresponding BSON format
     */
    public static BsonValue getAsBson( PolyValue obj, PolyType type, GridFSBucket bucket ) {
        if ( obj instanceof List ) {
            BsonArray array = new BsonArray();
            obj.asList().forEach( (Consumer<? super PolyValue>) el -> array.add( getAsBson( el, type, bucket ) ) );
            return array;
        } else if ( obj == null ) {
            return new BsonNull();
        }
        return switch ( type ) {
            case BIGINT -> handleBigInt( obj );
            case DECIMAL -> handleDecimal( obj );
            case TINYINT -> handleTinyInt( obj );
            case SMALLINT -> handleSmallInt( obj );
            case INTEGER -> handleInteger( obj );
            case FLOAT, REAL -> new BsonDouble( Double.parseDouble( obj.toString() ) );
            case DOUBLE -> handleDouble( obj );
            case DATE -> handleDate( obj );
            case TIME -> handleTime( obj );
            case TIMESTAMP -> handleTimestamp( obj );
            case BOOLEAN -> new BsonBoolean( obj.asBoolean().value );
            case BINARY -> new BsonString( new ByteString( obj.asBinary().value ).toBase64String() );
            case AUDIO, IMAGE, VIDEO, FILE -> handleMultimedia( bucket, obj );
            case INTERVAL -> handleInterval( obj );
            case JSON -> handleDocument( obj );
            default -> new BsonString( obj.toString() );
        };
    }


    /**
     * Retrieval method to get a suitable transformer, which transforms the untyped input
     * into the corresponding Bson format.
     *
     * @param types the corresponding type of the input object
     * @param bucket the bucket can be used to retrieve multimedia objects
     * @return the transformer method, which can be used to get the correct BsonValues
     */
    public static Function<PolyValue, BsonValue> getBsonTransformer( Queue<PolyType> types, GridFSBucket bucket ) {
        Function<PolyValue, BsonValue> function = getBsonTransformerPrimitive( types, bucket );
        return ( o ) -> {
            if ( o == null || o.isNull() ) {
                return new BsonNull();
            } else {
                return function.apply( o );
            }
        };
    }


    /**
     * Retrieval method to get a suitable transformer, which transforms the untyped input
     * into the corresponding Bson format according to a provided PolyType.
     *
     * @param types the corresponding type of the input object
     * @param bucket the bucket can be used to retrieve multimedia objects
     * @return the transformer method, which can be used to get the correct BsonValues
     */
    private static Function<PolyValue, BsonValue> getBsonTransformerPrimitive( Queue<PolyType> types, GridFSBucket bucket ) {
        return switch ( Objects.requireNonNull( types.poll() ) ) {
            case BIGINT -> BsonUtil::handleBigInt;
            case DECIMAL -> BsonUtil::handleDecimal;
            case TINYINT -> BsonUtil::handleTinyInt;
            case SMALLINT -> BsonUtil::handleSmallInt;
            case INTEGER -> BsonUtil::handleInteger;
            case FLOAT, REAL -> BsonUtil::handleNonDouble;
            case DOUBLE -> BsonUtil::handleDouble;
            case DATE -> BsonUtil::handleDate;
            case TIME -> BsonUtil::handleTime;
            case TIMESTAMP -> BsonUtil::handleTimestamp;
            case BOOLEAN -> BsonUtil::handleBoolean;
            case BINARY -> BsonUtil::handleBinary;
            case AUDIO, IMAGE, VIDEO, FILE -> ( o ) -> handleMultimedia( bucket, o );
            case INTERVAL -> BsonUtil::handleInterval;
            case JSON -> BsonUtil::handleDocument;
            case ARRAY -> {
                Function<PolyValue, BsonValue> transformer = getBsonTransformer( types, bucket );
                yield ( o ) -> new BsonArray( o.asList().stream().map( e -> transformer.apply( (PolyValue) e ) ).toList() );
            }
            case DOCUMENT -> o -> BsonDocument.parse( "{ k:" + (o.isString() ? o.asString().toQuotedJson() : o.toJson()) + "}" ).get( "k" );
            default -> BsonUtil::handleString;
        };
    }


    private static BsonValue handleString( PolyValue obj ) {
        return new BsonString( obj.toString() );
    }


    private static BsonValue handleNonDouble( PolyValue obj ) {
        return new BsonDouble( Double.parseDouble( obj.toString() ) );
    }


    private static BsonValue handleBinary( Object obj ) {
        return new BsonString( ((ByteString) obj).toBase64String() );
    }


    private static Object getObjFromRex( Object obj, Function<RexLiteral, Object> transformer ) {
        if ( obj instanceof RexLiteral ) {
            obj = transformer.apply( (RexLiteral) obj );
        }
        return obj;
    }


    private static BsonValue handleDocument( PolyValue obj ) {
        return BsonDocument.parse( obj.asDocument().toTypedJson() );
    }


    private static BsonValue handleBoolean( PolyValue obj ) {
        return new BsonBoolean( obj.asBoolean().value );
    }


    private static BsonValue handleBigInt( PolyValue obj ) {
        return new BsonInt64( obj.asNumber().longValue() );
    }


    private static BsonValue handleDouble( PolyValue obj ) {
        return new BsonDouble( obj.asNumber().DoubleValue() );
    }


    private static BsonValue handleMultimedia( GridFSBucket bucket, PolyValue o ) {
        ObjectId id = bucket.uploadFromStream( "_", o.asBlob().asBinaryStream() );
        return new BsonDocument()
                .append( "_type", new BsonString( "s" ) )
                .append( "_id", new BsonString( id.toString() ) );
    }


    private static BsonValue handleInterval( PolyValue obj ) {
        return new BsonDocument() {{
            this.put( "m", new BsonInt64( obj.asInterval().getMonths() ) );
            this.put( "ms", new BsonInt64( obj.asInterval().getMillis() ) );
        }};
    }



    private static BsonValue handleDecimal( PolyValue obj ) {
        return new BsonDecimal128( new Decimal128( obj.asNumber().BigDecimalValue() ) );
    }


    private static BsonValue handleTinyInt( PolyValue obj ) {
        return new BsonInt32( obj.asNumber().IntValue() );
    }


    private static BsonValue handleSmallInt( PolyValue obj ) {
        return new BsonInt32( obj.asNumber().IntValue() );
    }


    private static BsonValue handleDate( PolyValue obj ) {
        return new BsonInt64( obj.asTemporal().getMillisSinceEpoch() );
    }


    private static BsonValue handleTime( PolyValue obj ) {
        return new BsonInt64( obj.asTemporal().getMillisSinceEpoch() );
    }


    private static BsonValue handleTimestamp( PolyValue obj ) {
        return new BsonInt64( obj.asTemporal().getMillisSinceEpoch() );
    }


    private static BsonValue handleInteger( PolyValue obj ) {
        return new BsonInt32( obj.asNumber().IntValue() );
    }


    /**
     * Wrapper method, which unpacks the provided literal and retrieves it a BsonValue.
     *
     * @param literal the literal to transform to BSON
     * @param bucket the bucket, which can be used to retrieve multimedia objects
     * @return the transformed literal in BSON format
     */
    public static BsonValue getAsBson( RexLiteral literal, GridFSBucket bucket ) {
        return getAsBson( literal.value, literal.getType().getPolyType(), bucket );
    }


    /**
     * Recursively transforms a provided RexCall into a matching BsonArray.
     *
     * @param call the call which is transformed
     * @param bucket the bucket, which is used to retrieve multimedia objects
     * @return the transformed BsonArray
     */
    public static BsonArray getBsonArray( RexCall call, GridFSBucket bucket ) {
        BsonArray array = new BsonArray();
        for ( RexNode op : call.operands ) {
            if ( op instanceof RexCall ) {
                array.add( getBsonArray( (RexCall) op, bucket ) );
            } else {
                PolyType type = call.getType().getComponentType().getPolyType();
                array.add( getAsBson( ((RexLiteral) op).value, type, bucket ) );
            }
        }
        return array;
    }


    /**
     * Get the corresponding MongoDB class for a provided type.
     *
     * @param type the type, for which a class is needed
     * @return the supported class
     */
    public static Class<?> getClassFromType( PolyType type ) {
        return switch ( type ) {
            case BOOLEAN -> Boolean.class;
            case TINYINT -> Short.class;
            case SMALLINT, INTEGER -> Integer.class;
            case BIGINT -> Long.class;
            case DECIMAL -> BigDecimal.class;
            case FLOAT, REAL -> Float.class;
            case DOUBLE -> Double.class;
            case DATE -> Date.class;
            case TIME -> Time.class;
            case TIMESTAMP -> Timestamp.class;
            case CHAR, VARCHAR, BINARY, VARBINARY -> String.class;
            case FILE, IMAGE, VIDEO, AUDIO -> PushbackInputStream.class;
            default -> throw new IllegalStateException( "Unexpected value: " + type );
        };
    }


    /**
     * Helper method to transform an SQL like clause into the matching regex
     * _ {@code ->} .
     * % {@code ->} .*
     *
     * @param input the like clause as string
     * @return a Bson object which matches the initial like clause
     */
    public static BsonValue replaceLikeWithRegex( String input ) {
        if ( !input.startsWith( "%" ) ) {
            input = "^" + input;
        }

        if ( !input.endsWith( "%" ) ) {
            input = input + "$";
        }

        input = input
                .replace( "_", "." )
                .replace( "%", ".*" );

        return new BsonDocument()
                .append( "$regex", new BsonString( input ) )
                // Polypheny is case-insensitive and therefore we have to set the "i" option
                .append( "$options", new BsonString( "i" ) );
    }


    /**
     * Helper method to transform a BsonDocument to the matching Document which is needed
     * form some methods ( @see insertMany() ).
     *
     * @param bson the BsonDocument
     * @return the transformed BsonDocument as Document
     */
    public static Document asDocument( BsonDocument bson ) {
        Document doc = new Document();
        doc.putAll( bson );
        return doc;
    }


    /**
     * Method to retrieve the type numbers according to the MongoDB specifications
     * <a href="https://docs.mongodb.com/manual/reference/operator/query/type/">...</a>
     *
     * @param type PolyType which is matched
     * @return the corresponding type number for MongoDB
     */
    public static int getTypeNumber( PolyType type ) {
        return switch ( type ) {
            case BOOLEAN -> 8;
            case TINYINT, SMALLINT, INTEGER -> 16;
            case BIGINT, DATE, TIME, TIMESTAMP -> 18;
            case DECIMAL -> 19;
            case FLOAT, REAL, DOUBLE -> 1;
            case CHAR, VARCHAR, BINARY, VARBINARY -> 2;
            default -> throw new IllegalStateException( "Unexpected value: " + type );
        };
    }


    public static RexLiteral getAsLiteral( BsonValue value, RexBuilder rexBuilder ) {
        AlgDataType type = getTypeFromBson( value.getBsonType(), rexBuilder.getTypeFactory() );

        return (RexLiteral) rexBuilder.makeLiteral( getUnderlyingValue( value ), type, false );
    }


    private static <T extends Comparable<T>> Comparable<?> getUnderlyingValue( BsonValue value ) {
        switch ( value.getBsonType() ) {
            case NULL:
                return null;
            case STRING:
                return value.asString().getValue();
            case INT32:
                return value.asInt32().getValue();
            case INT64:
                return value.asInt64().getValue();
            case DOUBLE:
                return value.asDouble().doubleValue();
            case BINARY:
                return new ByteString( value.asBinary().getData() );
            case BOOLEAN:
                return value.asBoolean().getValue();
            case ARRAY:
                return ComparableList.copyOf( value.asArray().stream().map( BsonUtil::getUnderlyingValue ).map( e -> (T) e ).toList().listIterator() );
            case DOCUMENT:
                FlatMap<String, Comparable<?>> map = new FlatMap<>();
                value.asDocument().forEach( ( key, val ) -> map.put( key, getUnderlyingValue( val ) ) );
                return map;
            case DATE_TIME:
                return value.asDateTime().getValue();
            case TIMESTAMP:
                return value.asTimestamp().getValue();
            case DECIMAL128:
                return value.asDecimal128().decimal128Value().bigDecimalValue();
            default:
                throw new GenericRuntimeException( "The used Bson type is not supported." );
        }
    }


    public static AlgDataType getTypeFromBson( BsonType type, AlgDataTypeFactory factory ) {
        return switch ( type ) {
            case INT32, DECIMAL128, INT64, DOUBLE -> factory.createPolyType( PolyType.DECIMAL );
            case BINARY -> factory.createPolyType( PolyType.BINARY );
            case BOOLEAN -> factory.createPolyType( PolyType.BOOLEAN );
            case ARRAY -> factory.createArrayType( factory.createPolyType( PolyType.ANY ), -1 );
            case DOCUMENT -> factory.createMapType( factory.createPolyType( PolyType.ANY ), factory.createPolyType( PolyType.ANY ) );
            case DATE_TIME, TIMESTAMP -> factory.createPolyType( PolyType.TIMESTAMP );
            default -> factory.createPolyType( PolyType.ANY );
        };
    }


    public static String transformToBsonString( Map<RexLiteral, RexLiteral> map ) {
        return transformToBson( map ).toJson( JsonWriterSettings.builder()/*.outputMode( JsonMode.EXTENDED )*/.build() );
    }


    public static Document transformToBson( Map<RexLiteral, RexLiteral> map ) {
        Document doc = new Document();

        for ( Entry<RexLiteral, RexLiteral> entry : map.entrySet() ) {
            assert entry.getKey().getPolyType() == PolyType.CHAR;
            doc.put( entry.getKey().value.asString().value, getAsBson( entry.getValue(), null ) );
        }
        return doc;

    }


    public static Comparable<?> getAsObject( BsonValue value ) {
        switch ( value.getBsonType() ) {
            case END_OF_DOCUMENT, TIMESTAMP, MIN_KEY, MAX_KEY, UNDEFINED, OBJECT_ID, DATE_TIME, JAVASCRIPT_WITH_SCOPE, SYMBOL, JAVASCRIPT, DB_POINTER, REGULAR_EXPRESSION:
                break;
            case DOUBLE:
                return value.asDouble().decimal128Value();
            case STRING:
                return value.asString().getValue();
            case DOCUMENT:
                return value.asDocument().toJson();
            case ARRAY:
                return Arrays.toString( value.asArray().toArray() );
            case BINARY:
                return value.asBinary().asUuid();
            case BOOLEAN:
                return value.asBoolean().getValue();
            case NULL:
                return null;
            case INT32:
                return value.asInt32().getValue();
            case INT64:
                return value.asInt64().getValue();
            case DECIMAL128:
                return BigDecimal.valueOf( value.asDecimal128().doubleValue() );

        }
        throw new GenericRuntimeException( "BsonType cannot be transformed." );
    }


    public static Expression asExpression( BsonValue value ) {
        switch ( value.getBsonType() ) {

            case END_OF_DOCUMENT:
            case REGULAR_EXPRESSION:
            case DATE_TIME:
            case OBJECT_ID:
            case BINARY:
            case UNDEFINED:
            case DB_POINTER:
            case JAVASCRIPT:
            case SYMBOL:
            case JAVASCRIPT_WITH_SCOPE:
            case MIN_KEY:
            case MAX_KEY:
                break;
            case DOUBLE:
                return Expressions.constant( value.asDouble().getValue() );
            case STRING:
                return Expressions.constant( value.asString().getValue() );
            case DOCUMENT:
                List<Expression> literals = new ArrayList<>();
                for ( Entry<String, BsonValue> doc : value.asDocument().entrySet() ) {
                    Expression key = Expressions.constant( doc.getKey() );
                    Expression val = asExpression( doc.getValue() );

                    literals.add( Expressions.call( Pair.class, "of", key, val ) );
                }
                return Expressions.call( EnumUtils.class, "ofEntries", literals );

            case ARRAY:
                List<Expression> array = new ArrayList<>();

                for ( BsonValue doc : value.asArray() ) {
                    array.add( asExpression( doc ) );
                }
                return EnumUtils.expressionFlatList( array, Object.class );
            case BOOLEAN:
                return Expressions.constant( value.asBoolean().getValue() );
            case NULL:
                return Expressions.constant( null );
            case INT32:
                return Expressions.constant( value.asInt32().getValue() );
            case TIMESTAMP:
                return Expressions.constant( TimestampString.fromMillisSinceEpoch( value.asTimestamp().getValue() ) );
            case INT64:
                return Expressions.constant( value.asInt64().getValue() );
            case DECIMAL128:
                return Expressions.constant( value.asDecimal128().getValue().bigDecimalValue() );
        }

        throw new NotImplementedException();
    }


    public static PolyValue toPolyValue( BsonValue input ) {
        switch ( input.getBsonType() ) {
            case END_OF_DOCUMENT, MAX_KEY, MIN_KEY, TIMESTAMP, JAVASCRIPT_WITH_SCOPE, SYMBOL, JAVASCRIPT, DB_POINTER, REGULAR_EXPRESSION, DATE_TIME, OBJECT_ID, UNDEFINED, BINARY:
                break;
            case DOUBLE:
                return PolyDouble.of( input.asDouble().getValue() );
            case STRING:
                return PolyString.of( input.asString().getValue() );
            case DOCUMENT:
                Map<PolyString, PolyValue> document = new HashMap<>();

                for ( Entry<String, BsonValue> entry : input.asDocument().entrySet() ) {
                    document.put( PolyString.of( entry.getKey() ), toPolyValue( entry.getValue() ) );
                }

                return new PolyDocument( document );
            case ARRAY:
                PolyList<PolyValue> list = new PolyList<>();

                for ( BsonValue value : input.asArray() ) {

                    list.add( toPolyValue( value ) );
                }
                return list;
            case BOOLEAN:
                return PolyBoolean.of( input.asBoolean().getValue() );
            case NULL:
                return PolyNull.NULL;
            case INT32:
                return PolyInteger.of( input.asInt32().intValue() );
            case INT64:
                return PolyLong.of( input.asInt64().getValue() );
            case DECIMAL128:
                return PolyBigDecimal.of( input.asDecimal128().getValue().bigDecimalValue() );
        }
        throw new GenericRuntimeException( "Not considered: " + input.getBsonType() );
    }

}
