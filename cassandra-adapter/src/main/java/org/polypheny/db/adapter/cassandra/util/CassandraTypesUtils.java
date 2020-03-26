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
import org.polypheny.db.type.PolyType;


@Slf4j
public class CassandraTypesUtils {

    public static DataType getDataType( PolyType polyType ) {
        switch ( polyType ) {
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
                throw new RuntimeException( "Unable to convert sql type: " + polyType.getName() );
        }
    }


    public static PolyType getPolyType( DataType dataType ) {
        if ( dataType == DataTypes.UUID || dataType == DataTypes.TIMEUUID ) {
            return PolyType.CHAR;
        } else if ( dataType == DataTypes.ASCII || dataType == DataTypes.TEXT ) {
            return PolyType.VARCHAR;
        } else if ( dataType == DataTypes.INT || dataType == DataTypes.VARINT ) {
            return PolyType.INTEGER;
        } else if ( dataType == DataTypes.BIGINT ) {
            return PolyType.BIGINT;
        } else if ( dataType == DataTypes.DOUBLE ) {
            return PolyType.DOUBLE;
        } else if ( dataType == DataTypes.FLOAT ) {
            return PolyType.FLOAT;
        } else if ( dataType == DataTypes.TIME ) {
            return PolyType.TIME;
        } else if ( dataType == DataTypes.DATE ) {
            return PolyType.DATE;
        } else if ( dataType == DataTypes.TIMESTAMP ) {
            return PolyType.TIMESTAMP;
        } else {
            log.warn( "Unable to find type for cql type: {}. Returning ANY.", dataType );
            return PolyType.ANY;
        }
    }

}
