/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.util;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import lombok.extern.slf4j.Slf4j;


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
}
