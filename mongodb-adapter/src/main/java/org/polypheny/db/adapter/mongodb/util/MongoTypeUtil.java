/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb.util;

import com.mongodb.client.gridfs.GridFSBucket;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import org.apache.calcite.avatica.util.ByteString;
import org.bson.BsonArray;
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
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


/**
 * Helper class, which provided multiple methods to transform PolyTypes to the correct Bson representation
 */
public class MongoTypeUtil {

    /**
     * Retrieval method to get a suitable transformer, which transforms the untyped input
     * into the corresponding Bson format.
     *
     * @param type the corresponding type of the input object
     * @param bucket the bucket can be used to retrieve multimedia objects
     * @return the transformer method, which can be used to get the correct BsonValues
     */
    public static Function<Object, BsonValue> getBsonTransformer( PolyType type, GridFSBucket bucket ) {
        Function<Object, BsonValue> function = getBsonTransformerPrimitive( type, bucket );
        return ( o ) -> {
            if ( o == null ) {
                return new BsonNull();
            } else if ( o instanceof List ) {
                BsonArray array = new BsonArray();
                ((List<?>) o).forEach( el -> array.add( getAsBson( el, type, bucket ) ) );
                return array;
            } else {
                return function.apply( o );
            }
        };
    }


    /**
     * Retrieval method to get a suitable transformer, which transforms the untyped input
     * into the corresponding Bson format according to a provided PolyType.
     *
     * @param type the corresponding type of the input object
     * @param bucket the bucket can be used to retrieve multimedia objects
     * @return the transformer method, which can be used to get the correct BsonValues
     */
    private static Function<Object, BsonValue> getBsonTransformerPrimitive( PolyType type, GridFSBucket bucket ) {
        switch ( type ) {
            case BIGINT:
                return ( o ) -> new BsonInt64( (Long) o );
            case DECIMAL:
                return MongoTypeUtil::handleDecimal;
            case TINYINT:
                return MongoTypeUtil::handleTinyInt;
            case SMALLINT:
                return MongoTypeUtil::handleSmallInt;
            case INTEGER:
                return MongoTypeUtil::handleInteger;
            case FLOAT:
            case REAL:
                return ( o ) -> new BsonDouble( Double.parseDouble( o.toString() ) );
            case DOUBLE:
                return ( o ) -> new BsonDouble( (Double) o );
            case DATE:
                return MongoTypeUtil::handleDate;
            case TIME:
                return MongoTypeUtil::handleTime;
            case TIMESTAMP:
                return MongoTypeUtil::handleTimestamp;
            case BOOLEAN:
                return ( o ) -> new BsonBoolean( (Boolean) o );
            case BINARY:
                return ( o ) -> new BsonString( ((ByteString) o).toBase64String() );
            case SOUND:
            case IMAGE:
            case VIDEO:
            case FILE:
                return ( o ) -> handleMultimedia( bucket, (InputStream) o );
            case CHAR:
            case VARCHAR:
            default:
                return ( o ) -> new BsonString( o.toString() );
        }
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
    public static BsonValue getAsBson( Object obj, PolyType type, GridFSBucket bucket ) {
        if ( obj instanceof List ) {
            BsonArray array = new BsonArray();
            ((List<?>) obj).forEach( el -> array.add( getAsBson( el, type, bucket ) ) );
            return array;
        } else if ( obj == null ) {
            return new BsonNull();
        }
        switch ( type ) {
            case BIGINT:
                return new BsonInt64( (Long) obj );
            case DECIMAL:
                return handleDecimal( obj );
            case TINYINT:
                return handleTinyInt( obj );
            case SMALLINT:
                return handleSmallInt( obj );

            case INTEGER:
                return handleInteger( obj );
            case FLOAT:
            case REAL:
                return new BsonDouble( Double.parseDouble( obj.toString() ) );
            case DOUBLE:
                return new BsonDouble( (Double) obj );
            case DATE:
                return handleDate( obj );
            case TIME:
                return handleTime( obj );
            case TIMESTAMP:
                return handleTimestamp( obj );
            case BOOLEAN:
                return new BsonBoolean( (Boolean) obj );
            case BINARY:
                return new BsonString( ((ByteString) obj).toBase64String() );
            case SOUND:
            case IMAGE:
            case VIDEO:
            case FILE:
                return handleMultimedia( bucket, (InputStream) obj );
            case CHAR:
            case VARCHAR:
            default:
                return new BsonString( obj.toString() );
        }
    }


    private static BsonValue handleMultimedia( GridFSBucket bucket, InputStream o ) {
        ObjectId id = bucket.uploadFromStream( "_", o );
        return new BsonDocument()
                .append( "_type", new BsonString( "s" ) )
                .append( "_id", new BsonString( id.toString() ) );
    }


    private static BsonValue handleDecimal( Object o ) {
        if ( o instanceof String ) {
            return new BsonDecimal128( new Decimal128( new BigDecimal( (String) o ) ) );
        } else {
            return new BsonDecimal128( new Decimal128( new BigDecimal( String.valueOf( o ) ) ) );
        }
    }


    private static BsonValue handleTinyInt( Object o ) {
        if ( o instanceof Long ) {
            return new BsonInt32( Math.toIntExact( (Long) o ) );
        } else {
            return new BsonInt32( (Byte) o );
        }
    }


    private static BsonValue handleSmallInt( Object o ) {
        if ( o instanceof Long ) {
            return new BsonInt32( Math.toIntExact( (Long) o ) );
        } else if ( o instanceof Integer ) {
            return new BsonInt32( (Integer) o );
        } else {
            return new BsonInt32( (Short) o );
        }
    }


    private static BsonValue handleDate( Object o ) {
        if ( o instanceof Integer ) {
            return new BsonInt64( (Integer) o );
        } else if ( o instanceof Date ) {
            return new BsonInt64( ((Date) o).toLocalDate().toEpochDay() );
        } else {
            return new BsonInt64( new Date( ((Time) o).getTime() ).toLocalDate().toEpochDay() );
        }
    }


    private static BsonValue handleTime( Object o ) {
        if ( o instanceof Integer ) {
            return new BsonInt64( ((Integer) o) );
        } else {
            return new BsonInt64( ((Time) o).toLocalTime().toNanoOfDay() / 1000000 );
        }
    }


    private static BsonValue handleTimestamp( Object o ) {
        if ( o instanceof Timestamp ) {
            // timestamp do factor in the timezones, which means that 10:00 is 9:00 with
            // an one hour shift, as we lose this timezone information on retrieval
            // we have to include it into the time itself
            int offset = Calendar.getInstance().getTimeZone().getRawOffset();
            return new BsonInt64( ((Timestamp) o).getTime() + offset );
        } else if ( o instanceof Calendar ) {
            return new BsonInt64( ((Calendar) o).getTime().getTime() );
        } else {
            return new BsonInt64( (Long) o );
        }
    }


    private static BsonValue handleInteger( Object obj ) {
        if ( obj instanceof Long ) {
            return new BsonInt32( ((Long) obj).intValue() );
        } else {
            return new BsonInt32( (Integer) obj );
        }
    }


    /**
     * Wrapper method, which unpacks the provided literal and retrieves it a BsonValue.
     *
     * @param literal the literal to transform to BSON
     * @param bucket the bucket, which can be used to retrieve multimedia objects
     * @return the transformed literal in BSON format
     */
    public static BsonValue getAsBson( RexLiteral literal, GridFSBucket bucket ) {
        return getAsBson( getMongoComparable( literal.getType().getPolyType(), literal ), literal.getType().getPolyType(), bucket );
    }


    /**
     * Helper method which maps the RexLiteral to the provided type, which is MongoDB adapter conform.
     *
     * @param finalType the type which should be retrieved from the literal
     * @param el the literal itself
     * @return a MongoDB adapter compatible Comparable
     */
    public static Comparable<?> getMongoComparable( PolyType finalType, RexLiteral el ) {
        if ( el.getValue() == null ) {
            return null;
        }

        switch ( finalType ) {

            case BOOLEAN:
                return el.getValueAs( Boolean.class );
            case TINYINT:
                return el.getValueAs( Byte.class );
            case SMALLINT:
                return el.getValueAs( Short.class );
            case INTEGER:
            case DATE:
            case TIME:
                return el.getValueAs( Integer.class );
            case BIGINT:
            case TIMESTAMP:
                return el.getValueAs( Long.class );
            case DECIMAL:
                return el.getValueAs( BigDecimal.class ).toString();
            case FLOAT:
            case REAL:
                return el.getValueAs( Float.class );
            case DOUBLE:
                return el.getValueAs( Double.class );
            case CHAR:
            case VARCHAR:
                return el.getValueAs( String.class );
            case GEOMETRY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return el.getValueAs( ByteString.class ).toBase64String();
            default:
                return el.getValue();
        }
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
                array.add( getAsBson( getMongoComparable( type, (RexLiteral) op ), type, bucket ) );
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
        switch ( type ) {
            case BOOLEAN:
                return Boolean.class;
            case TINYINT:
                return Short.class;
            case SMALLINT:
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case DECIMAL:
                return BigDecimal.class;
            case FLOAT:
            case REAL:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case DATE:
                return Date.class;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return Time.class;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.class;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return String.class;
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return PushbackInputStream.class;
            default:
                throw new IllegalStateException( "Unexpected value: " + type );
        }
    }


    /**
     * Helper method to transform a SQL like clause into the matching regex
     * _ -> .
     * % -> .*
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
                // Polypheny is case insensitive and therefore we have to set the "i" option
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
        for ( Entry<String, BsonValue> entry : bson.entrySet() ) {
            doc.put( entry.getKey(), entry.getValue() );
        }
        return doc;
    }


    /**
     * Method to retrieve the type numbers according to the MongoDB specifications
     * https://docs.mongodb.com/manual/reference/operator/query/type/
     *
     * @param type PolyType which is matched
     * @return the corresponding type number for MongoDB
     */
    public static int getTypeNumber( PolyType type ) {
        switch ( type ) {
            case BOOLEAN:
                return 8;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return 16;
            case BIGINT:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 18;
            case DECIMAL:
                return 19;
            case FLOAT:
            case REAL:
            case DOUBLE:
                return 1;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return 2;
            default:
                throw new IllegalStateException( "Unexpected value: " + type );
        }
    }

}
