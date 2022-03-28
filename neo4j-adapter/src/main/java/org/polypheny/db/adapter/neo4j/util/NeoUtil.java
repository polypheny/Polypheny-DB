/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.function.Function1;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public interface NeoUtil {

    static Object asString( Object o, AlgDataType type ) {
        return asString( o, type.getPolyType(), getComponentTypeOrParent( type ) );
    }

    static Object asString( Object o, PolyType type, PolyType componentType ) {
        return o.toString();
    }

    static Function1<Value, Object> getTypeFunction( PolyType type, PolyType componentType ) {

        switch ( type ) {
            case BOOLEAN:
                return Value::asBoolean;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Value::asInt;
            case BIGINT:
                return Value::asLong;
            case DECIMAL:
            case FLOAT:
                return Value::asFloat;
            case REAL:
            case DOUBLE:
                return Value::asDouble;
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
            case VARCHAR:
                return Value::asString;
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
            case DOCUMENT:
                break;
            case GRAPH:
                break;
            case NODE:
                break;
            case EDGE:
                break;
            case PATH:
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
            case FILE:
                break;
            case IMAGE:
                break;
            case VIDEO:
                break;
            case SOUND:
                break;
            case JSON:
                break;
        }

        throw new RuntimeException( String.format( "Object of type %s was not transformable.", type ) );
    }

    static Function1<Record, Object> getTypesFunction( List<PolyType> types, List<PolyType> componentTypes ) {
        int i = 0;
        List<Function1<Value, Object>> functions = new ArrayList<>();
        for ( PolyType type : types ) {
            functions.add( getTypeFunction( type, componentTypes.get( i ) ) );
            i++;
        }
        if ( functions.size() == 1 ) {
            // SCALAR
            return o -> functions.get( 0 ).apply( o.get( 0 ) );
        }

        // ARRAY
        return o -> Pair.zip( o.fields(), functions ).stream().map( e -> e.right.apply( e.left.value() ) ).collect( Collectors.toList() ).toArray();
    }

    static String asParameter( long key, boolean withDollar ) {
        if ( withDollar ) {
            return String.format( "$p%s", key );
        } else {
            return String.format( "p%s", key );
        }
    }


    static PolyType getComponentTypeOrParent( AlgDataType type ) {
        if ( type.getPolyType() == PolyType.ARRAY ) {
            return type.getComponentType().getPolyType();
        }
        return type.getPolyType();
    }

    static String rexAsString( RexLiteral value ) {
        return value.getValueForQueryParameterizer().toString();
    }

    class PhysicalMapping {

    }

}
