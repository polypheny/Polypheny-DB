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
import org.polypheny.db.adapter.mongodb.MongoRel.Implementor;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

public class MongoTypeUtil {

    public static Function<Object, BsonValue> getBsonTransformer( PolyType type, GridFSBucket bucket ) {
        Function<Object, BsonValue> function = getBsonTransformerSpecial( type, bucket );
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


    private static Function<Object, BsonValue> getBsonTransformerSpecial( PolyType type, GridFSBucket bucket ) {
        switch ( type ) {
            case CHAR:
            case VARCHAR:
                return ( o ) -> new BsonString( o.toString() );
            case BIGINT:
                return ( o ) -> new BsonInt64( (Long) o );
            case DECIMAL:
                return ( o ) -> {
                    if ( o instanceof String ) {
                        return new BsonDecimal128( new Decimal128( new BigDecimal( (String) o ) ) );
                    } else {
                        return new BsonDecimal128( new Decimal128( new BigDecimal( String.valueOf( o ) ) ) );
                    }
                };

            case TINYINT:
                return ( o ) -> {
                    if ( o instanceof Long ) {
                        return new BsonInt32( Math.toIntExact( (Long) o ) );
                    } else {
                        return new BsonInt32( (Byte) o );
                    }
                };
            case SMALLINT:
                return ( o ) -> {
                    if ( o instanceof Long ) {
                        return new BsonInt32( Math.toIntExact( (Long) o ) );
                    } else if ( o instanceof Integer ) {
                        return new BsonInt32( (Integer) o );
                    } else {
                        return new BsonInt32( (Short) o );
                    }
                };
            case INTEGER:
                return ( o ) -> {
                    if ( o instanceof Long ) {
                        return new BsonInt32( ((Long) o).intValue() );
                    } else {
                        return new BsonInt32( (Integer) o );
                    }

                };
            case FLOAT:
            case REAL:
                return ( o ) -> new BsonDouble( Double.parseDouble( o.toString() ) );
            case DOUBLE:
                return ( o ) -> new BsonDouble( (Double) o );
            case DATE:
                return ( o ) -> {
                    if ( o instanceof Integer ) {
                        return new BsonInt64( (Integer) o );
                    } else if ( o instanceof Date ) {
                        return new BsonInt64( ((Date) o).toLocalDate().toEpochDay() );
                    } else {
                        return new BsonInt64( new Date( ((Time) o).getTime() ).toLocalDate().toEpochDay() );
                    }
                };
            case TIME:
                return ( o ) -> {
                    if ( o instanceof Integer ) {
                        return new BsonInt64( ((Integer) o) );
                    } else {
                        return new BsonInt64( ((Time) o).toLocalTime().toNanoOfDay() / 1000000 );
                    }
                };
            case TIMESTAMP:
                return ( o ) -> {
                    if ( o instanceof Timestamp ) {
                        // we have to adjust the timezone and add it to the time, as we lose it on retrieval
                        int offset = Calendar.getInstance().getTimeZone().getRawOffset();
                        return new BsonInt64( ((Timestamp) o).getTime() + offset );
                    } else if ( o instanceof Calendar ) {
                        return new BsonInt64( ((Calendar) o).getTime().getTime() );
                    } else {
                        return new BsonInt64( (Long) o );
                    }
                };
            case BOOLEAN:
                return ( o ) -> new BsonBoolean( (Boolean) o );
            case BINARY:
                return ( o ) -> new BsonString( ((ByteString) o).toBase64String() );
            case SOUND:
            case IMAGE:
            case VIDEO:
            case FILE:
                return ( o ) -> {
                    ObjectId id = bucket.uploadFromStream( "_", (InputStream) o );
                    return new BsonDocument()
                            .append( "_type", new BsonString( "s" ) )
                            .append( "_id", new BsonString( id.toString() ) );
                };
            default:
                return ( o ) -> new BsonString( o.toString() );
        }
    }


    public static BsonValue getAsBson( Object obj, PolyType type, GridFSBucket bucket ) {
        if ( obj instanceof List ) {
            BsonArray array = new BsonArray();
            ((List<?>) obj).forEach( el -> array.add( getAsBson( el, type, bucket ) ) );
            return array;
        } else if ( obj == null ) {
            return new BsonNull();
        }
        switch ( type ) {
            case CHAR:
            case VARCHAR:
                return new BsonString( obj.toString() );
            case BIGINT:
                return new BsonInt64( (Long) obj );
            case DECIMAL:
                if ( obj instanceof String ) {
                    return new BsonDecimal128( new Decimal128( new BigDecimal( (String) obj ) ) );
                } else {
                    return new BsonDecimal128( new Decimal128( new BigDecimal( String.valueOf( obj ) ) ) );
                }
            case TINYINT:
                if ( obj instanceof Long ) {
                    return new BsonInt32( Math.toIntExact( (Long) obj ) );
                } else {
                    return new BsonInt32( (Byte) obj );
                }
            case SMALLINT:
                if ( obj instanceof Long ) {
                    return new BsonInt32( Math.toIntExact( (Long) obj ) );
                } else if ( obj instanceof Integer ) {
                    return new BsonInt32( (Integer) obj );
                } else {
                    return new BsonInt32( (Short) obj );
                }

            case INTEGER:
                if ( obj instanceof Long ) {
                    return new BsonInt32( ((Long) obj).intValue() );
                } else {
                    return new BsonInt32( (Integer) obj );
                }
            case FLOAT:
            case REAL:
                return new BsonDouble( Double.parseDouble( obj.toString() ) );
            case DOUBLE:
                return new BsonDouble( (Double) obj );
            case DATE:
                if ( obj instanceof Integer ) {
                    return new BsonInt64( (Integer) obj );
                } else if ( obj instanceof Date ) {
                    return new BsonInt64( ((Date) obj).toLocalDate().toEpochDay() );
                } else {
                    return new BsonInt64( new Date( ((Time) obj).getTime() ).toLocalDate().toEpochDay() );
                }
            case TIME:
                if ( obj instanceof Integer ) {
                    return new BsonInt64( ((Integer) obj) );
                } else {
                    return new BsonInt64( ((Time) obj).toLocalTime().toNanoOfDay() / 1000000 );
                }
            case TIMESTAMP:
                if ( obj instanceof Timestamp ) {
                    int offset = Calendar.getInstance().getTimeZone().getRawOffset();
                    return new BsonInt64( ((Timestamp) obj).getTime() + offset );
                } else if ( obj instanceof Calendar ) {
                    return new BsonInt64( ((Calendar) obj).getTime().getTime() );
                } else {
                    return new BsonInt64( (Long) obj );
                }
            case BOOLEAN:
                return new BsonBoolean( (Boolean) obj );
            case BINARY:
                return new BsonString( ((ByteString) obj).toBase64String() );
            case SOUND:
            case IMAGE:
            case VIDEO:
            case FILE:
                ObjectId id = bucket.uploadFromStream( "_", (InputStream) obj );
                return new BsonDocument()
                        .append( "_type", new BsonString( "s" ) )
                        .append( "_id", new BsonString( id.toString() ) );
            default:
                return new BsonString( obj.toString() );
        }
    }


    public static BsonValue getAsBson( RexLiteral literal, GridFSBucket bucket ) {
        return getAsBson( getMongoComparable( literal.getType().getPolyType(), literal ), literal.getType().getPolyType(), bucket );
    }


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
                return el.getValueAs( Integer.class );
            case BIGINT:
                return el.getValueAs( Long.class );
            case DECIMAL:
                return el.getValueAs( BigDecimal.class ).toString();
            case FLOAT:
            case REAL:
                return el.getValueAs( Float.class );
            case DOUBLE:
                return el.getValueAs( Double.class );
            case DATE:
            case TIME:
                return el.getValueAs( Integer.class );
            case TIMESTAMP:
                return el.getValueAs( Long.class );
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
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                throw new RuntimeException( "Interval is not supported yet" );
            case CHAR:
            case VARCHAR:
                return String.class;
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


    public static BsonValue replaceRegex( String input ) {

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


    public static Document asDocument( BsonDocument bson ) {
        Document doc = new Document();
        for ( Entry<String, BsonValue> entry : bson.entrySet() ) {
            doc.put( entry.getKey(), entry.getValue() );
        }
        return doc;
    }


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
            /*case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return PushbackInputStream.class;*/
            default:
                throw new IllegalStateException( "Unexpected value: " + type );
        }
    }


    public static BsonDocument visit( RexNode left, Implementor preProjections ) {
        return null;
    }

}
