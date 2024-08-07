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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;


public class BsonUtil {

    private final static List<Pair<String, String>> mappings = new ArrayList<>();
    private final static List<String> stops = new ArrayList<>();
    public static final String DOC_MONTH_KEY = "m";
    public static final String DOC_MILLIS_KEY = "ms";
    public static final String DOC_MEDIA_TYPE_KEY = "_type";
    public static final String DOC_MEDIA_ID_KEY = "_id";


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
     * <p>
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
            case DECIMAL -> handleDecimal( obj, Optional.empty() );
            case TINYINT -> handleTinyInt( obj );
            case SMALLINT -> handleSmallInt( obj );
            case INTEGER -> handleInteger( obj );
            case FLOAT, REAL -> new BsonDouble( Double.parseDouble( obj.toString() ) );
            case DOUBLE -> handleDouble( obj, Optional.empty() );
            case DATE -> handleDate( obj );
            case TIME -> handleTime( obj );
            case TIMESTAMP -> handleTimestamp( obj );
            case BOOLEAN -> new BsonBoolean( obj.asBoolean().value );
            case BINARY, VARBINARY -> new BsonBinary( obj.asBinary().value );
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
    public static Function<PolyValue, BsonValue> getBsonTransformer( Queue<Pair<PolyType, Optional<Integer>>> types, GridFSBucket bucket ) {
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
    private static Function<PolyValue, BsonValue> getBsonTransformerPrimitive( Queue<Pair<PolyType, Optional<Integer>>> types, GridFSBucket bucket ) {
        Pair<PolyType, Optional<Integer>> type = types.poll();
        return switch ( Objects.requireNonNull( type.left ) ) {
            case BIGINT -> BsonUtil::handleBigInt;
            case DECIMAL -> obj -> handleDecimal( obj, type.right );
            case TINYINT -> BsonUtil::handleTinyInt;
            case SMALLINT -> BsonUtil::handleSmallInt;
            case INTEGER -> BsonUtil::handleInteger;
            case FLOAT, REAL -> obj -> handleNonDouble( obj, type.right );
            case DOUBLE -> obj -> handleDouble( obj, type.right );
            case DATE -> BsonUtil::handleDate;
            case TIME -> BsonUtil::handleTime;
            case TIMESTAMP -> BsonUtil::handleTimestamp;
            case BOOLEAN -> BsonUtil::handleBoolean;
            case BINARY, VARBINARY -> BsonUtil::handleBinary;
            case AUDIO, IMAGE, VIDEO, FILE -> ( o ) -> handleMultimedia( bucket, o );
            case INTERVAL -> BsonUtil::handleInterval;
            case JSON -> BsonUtil::handleDocument;
            case ARRAY -> {
                Function<PolyValue, BsonValue> transformer = getBsonTransformer( types, bucket );
                yield ( o ) -> new BsonArray( o.asList().stream().map( transformer ).toList() );
            }
            case DOCUMENT -> o -> BsonDocument.parse( "{ k:" + (o.isString() ? o.asString().toQuotedJson() : o.toJson()) + "}" ).get( "k" );
            default -> BsonUtil::handleString;
        };
    }


    private static BsonValue handleString( PolyValue obj ) {
        return new BsonString( obj.toString() );
    }


    private static BsonValue handleNonDouble( PolyValue obj, Optional<Integer> precision ) {
        return new BsonDouble( Double.parseDouble( obj.toString() ) );
    }


    private static BsonValue handleBinary( Object obj ) {
        if ( obj instanceof PolyBinary polyBinary ) {
            return new BsonBinary( polyBinary.value );
        } else if ( obj instanceof ByteString byteString ) {
            return new BsonBinary( byteString.getBytes() );
        } else if ( obj instanceof byte[] bytes ) {
            return new BsonBinary( bytes );
        }
        throw new GenericRuntimeException( "The provided object is not a binary object." );

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


    private static BsonValue handleDouble( PolyValue obj, Optional<Integer> precision ) {
        return new BsonDouble( obj.asNumber().DoubleValue() );
    }


    private static BsonValue handleMultimedia( GridFSBucket bucket, PolyValue o ) {
        if ( o.isBinary() ) {
            return new BsonBinary( o.asBinary().value );
        }
        ObjectId id = bucket.uploadFromStream( "_", o.asBlob().asBinaryStream() );
        return new BsonDocument()
                .append( DOC_MEDIA_TYPE_KEY, new BsonString( "s" ) )
                .append( DOC_MEDIA_ID_KEY, new BsonString( id.toString() ) );
    }


    private static BsonValue handleInterval( PolyValue obj ) {
        return new BsonDocument() {{
            this.put( DOC_MONTH_KEY, new BsonInt64( obj.asInterval().getMonths() ) );
            this.put( DOC_MILLIS_KEY, new BsonInt64( obj.asInterval().getMillis() ) );
        }};
    }


    private static BsonValue handleDecimal( PolyValue obj, Optional<Integer> precision ) {
        BigDecimal decimal = obj.asNumber().BigDecimalValue();
        decimal = precision.isPresent() ? decimal.setScale( precision.get(), RoundingMode.HALF_UP ) : decimal;
        return new BsonDecimal128( new Decimal128( decimal ) );
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
