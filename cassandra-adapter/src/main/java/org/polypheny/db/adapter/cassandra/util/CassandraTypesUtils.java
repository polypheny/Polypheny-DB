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

package org.polypheny.db.adapter.cassandra.util;


import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.cassandra.CassandraValues;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;


@Slf4j
public class CassandraTypesUtils {

    private static final Gson GSON;


    static {
        GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter( PolyType.class, PolyType.getSerializer() );
        GSON = gsonBuilder.create();
    }


    public static DataType getDataType( PolyType polyType, UserDefinedType arrayContainerUdt ) {
        switch ( polyType ) {
            case BOOLEAN:
                return DataTypes.BOOLEAN;
            case TINYINT:
                return DataTypes.TINYINT;
            case SMALLINT:
                return DataTypes.SMALLINT;
            case INTEGER:
                return DataTypes.INT;
            case BIGINT:
                return DataTypes.BIGINT;
            case DECIMAL:
                return DataTypes.DECIMAL;
            case FLOAT:
            case REAL:
                // TODO: What to return for real?
                return DataTypes.FLOAT;
            case DOUBLE:
                return DataTypes.DOUBLE;
            case DATE:
                return DataTypes.DATE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIME;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIMESTAMP;
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
                throw new RuntimeException( "Intervals are WIP." );
//                return DataTypes.DURATION;
            case CHAR:
                // TODO: What to return for char?
            case VARCHAR:
            case JSON:
                return DataTypes.TEXT;
            case BINARY:
            case VARBINARY:
                return DataTypes.BLOB;
            case ARRAY:
                return arrayContainerUdt;
            case NULL:
            case ANY:
            case SYMBOL:
            case MULTISET:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
                break;
        }

        throw new RuntimeException( "Unable to convert sql type: " + polyType.getName() );
    }


    public static PolyType getPolyType( DataType dataType ) {

        if ( dataType == DataTypes.UUID || dataType == DataTypes.TIMEUUID ) {
            return PolyType.CHAR;
        } else if ( dataType == DataTypes.ASCII || dataType == DataTypes.TEXT ) {
            return PolyType.VARCHAR;
        } else if ( dataType == DataTypes.TINYINT ) {
            return PolyType.TINYINT;
        } else if ( dataType == DataTypes.SMALLINT ) {
            return PolyType.SMALLINT;
        } else if ( dataType == DataTypes.INT || dataType == DataTypes.VARINT ) {
            return PolyType.INTEGER;
        } else if ( dataType == DataTypes.BIGINT ) {
            return PolyType.BIGINT;
        } else if ( dataType == DataTypes.DOUBLE ) {
            return PolyType.DOUBLE;
        } else if ( dataType == DataTypes.FLOAT ) {
            // TODO JS: Float vs real?
            return PolyType.FLOAT;
        } else if ( dataType == DataTypes.DECIMAL ) {
            return PolyType.DECIMAL;
        } else if ( dataType == DataTypes.TIME ) {
            return PolyType.TIME;
        } else if ( dataType == DataTypes.DATE ) {
            return PolyType.DATE;
        } else if ( dataType == DataTypes.TIMESTAMP ) {
            return PolyType.TIMESTAMP;
        } else if ( dataType == DataTypes.BLOB ) {
            return PolyType.VARBINARY;
        } else if ( dataType == DataTypes.BOOLEAN ) {
            return PolyType.BOOLEAN;
        } else if ( dataType instanceof UserDefinedType ) {
            return PolyType.ARRAY;
        } else {
            log.warn( "Unable to find type for cql type: {}. Returning ANY.", dataType );
            return PolyType.ANY;
        }
    }


    public static Class<?> getJavaType( DataType dataType ) {

        if ( dataType == DataTypes.ASCII ) {
            return String.class;
        } else if ( dataType == DataTypes.BIGINT ) {
            return Long.class;
        } else if ( dataType == DataTypes.BLOB ) {
            return java.nio.ByteBuffer.class;
        } else if ( dataType == DataTypes.BOOLEAN ) {
            return Boolean.class;
        } else if ( dataType == DataTypes.COUNTER ) {
            return Long.class;
        } else if ( dataType == DataTypes.DATE ) {
            return java.time.LocalDate.class;
        } else if ( dataType == DataTypes.DECIMAL ) {
            return java.math.BigDecimal.class;
        } else if ( dataType == DataTypes.DOUBLE ) {
            return Double.class;
        } else if ( dataType == DataTypes.DURATION ) {
            return CqlDuration.class;
        } else if ( dataType == DataTypes.FLOAT ) {
            return Float.class;
        } else if ( dataType == DataTypes.INET ) {
            return java.net.InetAddress.class;
        } else if ( dataType == DataTypes.INT ) {
            return Integer.class;
        } else if ( dataType == DataTypes.SMALLINT ) {
            return Short.class;
        } else if ( dataType == DataTypes.TEXT ) {
            return String.class;
        } else if ( dataType == DataTypes.TIME ) {
            return java.time.LocalTime.class;
        } else if ( dataType == DataTypes.TIMESTAMP ) {
            return java.time.Instant.class;
        } else if ( dataType == DataTypes.TIMEUUID ) {
            return java.util.UUID.class;
        } else if ( dataType == DataTypes.TINYINT ) {
            return Byte.class;
        } else if ( dataType == DataTypes.UUID ) {
            return java.util.UUID.class;
        } else if ( dataType == DataTypes.VARINT ) {
            return java.math.BigInteger.class;
        } else {
            log.warn( "Unable to find type for cql type: {}. Returning ANY.", dataType );
            return Object.class;
        }
    }


    static boolean canCastInternally( PolyType to, PolyType from ) {
        switch ( from ) {
            case BOOLEAN:
                switch ( to ) {
                    case CHAR:
                    case VARCHAR:
                        return true;
                    default:
                        return false;
                }
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                switch ( to ) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT:
                    case DOUBLE:
                    case DECIMAL:
                    case CHAR:
                    case VARCHAR:
                        return true;
                    default:
                        return false;
                }
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
                break;
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
        }

        return false;
    }


    public static Function<Object, Object> convertToFrom( PolyType to, PolyType from ) {
        Function<Object, Object> f = null;

        if ( to == from ) {
            return Function.identity();
        }

        switch ( from ) {
            case BOOLEAN:
                switch ( to ) {
                    case BOOLEAN:
                        f = Function.identity();
                        break;
                    case INTEGER:
                        f = boolIn -> (((Boolean) boolIn) ? 1 : 0);
                        break;
                    case CHAR:
                    case VARCHAR:
                        f = boolIn -> (((Boolean) boolIn) ? "true" : "false");
                        break;
                }
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                switch ( to ) {
                    case BOOLEAN:
                        f = intInt -> (((Integer) intInt) != 0);
                        break;
                    case FLOAT:
                    case DOUBLE:
                        f = intInt -> (((Integer) intInt).doubleValue());
                        break;
                    case CHAR:
                    case VARCHAR:
                        f = intInt -> (((Integer) intInt).toString());
                        break;
                }
                break;
            case DECIMAL:
                break;
            case FLOAT:
            case REAL:
            case DOUBLE:
                switch ( to ) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        f = doubleVal -> (((Double) doubleVal).intValue());
                        break;
                    case CHAR:
                    case VARCHAR:
                        f = doubleVal -> (((Double) doubleVal).toString());
                        break;
                }
                break;
            case DATE:
                break;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                break;
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
                break;
            case CHAR:
            case VARCHAR:
                switch ( to ) {
                    case BOOLEAN:
                        f = stringVal -> {
                            String string = (String) stringVal;
                            if ( "true".equalsIgnoreCase( string ) ) {
                                return true;
                            } else if ( "false".equalsIgnoreCase( string ) ) {
                                return false;
                            } else {
                                throw new IllegalArgumentException( "Unable to converter string \"" + string + "\" to boolean." );
                            }
                        };
                        break;
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        f = stringVal -> Integer.valueOf( (String) stringVal );
                        break;
                    case FLOAT:
                    case DOUBLE:
                        f = stringVal -> Double.parseDouble( (String) stringVal );
                        break;
                }
                break;
            case BINARY:
            case VARBINARY:
                break;
            case ANY:
            case SYMBOL:
            case MULTISET:
            case ARRAY:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
                break;
        }

        if ( f != null ) {
            return f;
        } else {
            throw new RuntimeException( "Unable to cast from " + from.getName() + " to " + to.getName() + "." );
        }
    }


    public static UdtValue createArrayContainerDataType( UserDefinedType arrayUdt, int dimension, int cardinality, PolyType innerType, RexCall arrayCall ) {
        return arrayUdt.newValue()
                .setString( 0, innerType.getTypeName() )
//                .setString( 0, GSON.toJson( innerType, PolyType.class ) )
                .setInt( 1, dimension )
                .setInt( 2, cardinality )
                .setString( 3, GSON.toJson( createListForArrays( arrayCall.operands ) ) );
    }


    public static List<Object> unparseArrayContainerUdt( UdtValue arrayContainer ) {
        if ( arrayContainer == null ) {
            return null;
        }

        PolyType innerType = GSON.fromJson( arrayContainer.getString( "innertype" ), PolyType.class );
        long dimension = (long) arrayContainer.getInt( "dimension" );
        int cardinality = arrayContainer.getInt( "cardinality" );
        Type conversionType = PolyTypeUtil.createNestedListType( dimension, innerType );
        String stringValue = arrayContainer.getString( "data" );
        if ( stringValue == null ) {
            return null;
        }
        return GSON.fromJson( stringValue.trim(), conversionType );
    }


    private static List<Object> createListForArrays( List<RexNode> operands ) {
        List<Object> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                Object value = CassandraValues.literalValue( (RexLiteral) node );
                list.add( value );
            } else if ( node instanceof RexCall ) {
                list.add( createListForArrays( ((RexCall) node).operands ) );
            } else {
                throw new RuntimeException( "Invalid array" );
            }
        }
        return list;
    }

}
