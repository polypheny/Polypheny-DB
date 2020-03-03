/*
 * Copyright 2019-2020 The Polypheny Project
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


import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolySqlType;
import org.polypheny.db.sql.type.SqlTypeName;


@Slf4j
public class CassandraTypesUtils {

    public static DataType getDataType( SqlTypeName sqlTypeName ) {
        switch ( sqlTypeName ) {
            case BOOLEAN:
                return DataTypes.BOOLEAN;
            case DATE:
                return DataTypes.DATE;
            case TIME:
                return DataTypes.TIME;
            case TIMESTAMP:
                return DataTypes.TIMESTAMP;
            case INTEGER:
                return DataTypes.INT;
            case DOUBLE:
                return DataTypes.DOUBLE;
            case FLOAT:
                return DataTypes.FLOAT;
            default:
                throw new RuntimeException( "Unable to convert sql type: " + sqlTypeName.getName() );
        }
    }


    public static SqlTypeName getSqlTypeName( DataType dataType ) {
        if ( dataType == DataTypes.UUID || dataType == DataTypes.TIMEUUID ) {
            return SqlTypeName.CHAR;
        } else if ( dataType == DataTypes.ASCII || dataType == DataTypes.TEXT ) {
            return SqlTypeName.VARCHAR;
        } else if ( dataType == DataTypes.INT || dataType == DataTypes.VARINT ) {
            return SqlTypeName.INTEGER;
        } else if ( dataType == DataTypes.BIGINT ) {
            return SqlTypeName.BIGINT;
        } else if ( dataType == DataTypes.DOUBLE ) {
            return SqlTypeName.DOUBLE;
        } else if ( dataType == DataTypes.FLOAT ) {
            return SqlTypeName.FLOAT;
        } else if ( dataType == DataTypes.TIME ) {
            return SqlTypeName.TIME;
        } else if ( dataType == DataTypes.DATE ) {
            return SqlTypeName.DATE;
        } else if ( dataType == DataTypes.TIMESTAMP ) {
            return SqlTypeName.TIMESTAMP;
        } else {
            log.warn( "Unable to find type for cql type: {}. Returning ANY.", dataType );
            return SqlTypeName.ANY;
        }
    }


    public static DataType getDataType( PolySqlType polySqlType ) {
        switch ( polySqlType ) {
            case BOOLEAN:
                return DataTypes.BOOLEAN;
            case VARBINARY:
                throw new RuntimeException( "Unsupported datatype: " + polySqlType.name() );
            case INTEGER:
                return DataTypes.INT;
            case BIGINT:
                return DataTypes.BIGINT;
            case REAL:
                throw new RuntimeException( "Unsupported datatype: " + polySqlType.name() );
            case DOUBLE:
                return DataTypes.DOUBLE;
            case DECIMAL:
                return DataTypes.DECIMAL;
            case VARCHAR:
                return DataTypes.TEXT;
            case TEXT:
                return DataTypes.TEXT;
            case DATE:
                return DataTypes.DATE;
            case TIME:
                return DataTypes.TIME;
            case TIMESTAMP:
                return DataTypes.TIMESTAMP;
        }
        throw new RuntimeException( "Unknown type: " + polySqlType.name() );
    }


    public static PolySqlType getPolySqlType( DataType dataType ) {
        if ( dataType == DataTypes.UUID || dataType == DataTypes.TIMEUUID ) {
            return PolySqlType.TEXT;
        } else if ( dataType == DataTypes.ASCII || dataType == DataTypes.TEXT ) {
            return PolySqlType.VARCHAR;
        } else if ( dataType == DataTypes.INT || dataType == DataTypes.VARINT ) {
            return PolySqlType.INTEGER;
        } else if ( dataType == DataTypes.BIGINT ) {
            return PolySqlType.BIGINT;
        } else if ( dataType == DataTypes.DOUBLE ) {
            return PolySqlType.DOUBLE;
        } else if ( dataType == DataTypes.TIME ) {
            return PolySqlType.TIME;
        } else if ( dataType == DataTypes.DATE ) {
            return PolySqlType.DATE;
        } else if ( dataType == DataTypes.TIMESTAMP ) {
            return PolySqlType.TIMESTAMP;
        } else {
            throw new RuntimeException( "Unknown type: " + dataType.toString() );
        }
    }


    public static Function<Object, Object> polySqlCastFunction( PolySqlType in, PolySqlType out ) {
        Function<Object, Object> converter = null;
        switch ( in ) {
            case BOOLEAN:
                switch ( out ) {
                    case BOOLEAN:
                        converter = Function.identity();
                        break;
                    case INTEGER:
                        converter = boolIn -> (((Boolean) boolIn) ? 1 : 0);
                        break;
                    case TEXT:
                        converter = boolIn -> (((Boolean) boolIn) ? "true" : "false");
                        break;
                }
                break;
            case VARBINARY:
                break;
            case INTEGER:
                break;
            case BIGINT:
                break;
            case REAL:
                break;
            case DOUBLE:
                break;
            case DECIMAL:
                break;
            case VARCHAR:
                break;
            case TEXT:
                break;
            case DATE:
                break;
            case TIME:
                break;
            case TIMESTAMP:
                break;
        }

        if ( converter != null ) {
            return converter;
        } else {
            throw new RuntimeException( "Unable to convert from " + in.name() + " to " + out.name() );
        }
    }


    //    public static <T> Pair<Class<T>, Function<Class<T>, Term>> convertDataType( PolySqlType in, PolySqlType out ) {
    public static <T> Function<Object, Term> convertDataType( PolySqlType in, PolySqlType out ) {
        Function<Object, Term> converter;
        switch ( in ) {
            case INTEGER: {
                switch ( out ) {
                    case INTEGER:
                        converter = QueryBuilder::literal;
                        break;
                    case DOUBLE:
                        converter = integer -> QueryBuilder.literal( ((Integer) integer).doubleValue() );
                        break;
                    case TEXT:
                        converter = integer -> QueryBuilder.literal( ((Integer) integer ).toString() );
                    default:
                        converter = null;
//                        throw new RuntimeException( "Unable to convert from " + in.name() + " to " + out.name() );
                }
            }
            break;
            case DOUBLE: {
                switch ( out ) {
                    case DOUBLE:
                        converter = QueryBuilder::literal;
                        break;
                    case INTEGER:
                        converter = doubleValue -> QueryBuilder.literal( ((Double) doubleValue).intValue() );
                        break;
                    default:
                        converter = null;
//                        throw new RuntimeException( "Unable to convert from " + in.name() + " to " + out.name() );
                }
            }
            break;
            default:
                converter = null;
//                throw new RuntimeException( "Unable to convert from " + in.name() + " to " + out.name() );
        }

        if ( converter != null ) {
            return converter;
        } else {
            throw new RuntimeException( "Unable to convert from " + in.name() + " to " + out.name() );
        }
    }
}
