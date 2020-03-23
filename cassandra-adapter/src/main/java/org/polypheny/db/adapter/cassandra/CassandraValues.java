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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;


@Slf4j
public class CassandraValues extends Values implements CassandraRel {

    public CassandraValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    /**
     * Convert the value of a literal to a string.
     *
     * @param literal Literal to translate
     * @return String representation of the literal
     */
    public static Object literalValue( RexLiteral literal ) {
        Object valueType = getJavaClass( literal );
        return valueType;
    }


    public static Object getJavaClass( RexLiteral literal ) {
        RelDataType type = literal.getType();
        if ( type instanceof BasicPolyType || type instanceof IntervalPolyType ) {
            switch ( type.getSqlTypeName() ) {
                case VARCHAR:
                case CHAR:
                    return literal.getValue2();
                case DATE:
                    try {
                        return LocalDate.parse( literal.getValueAs( DateString.class ).toString() );
                    } catch ( Exception e ) {
                        log.error( "Unable to cast date. ", e );
                        throw new RuntimeException( e );
                    }
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                    try {
                        log.info( "Attempting to convert date." );
                        return LocalTime.parse( literal.getValueAs( TimeString.class ).toString() );
                    } catch ( Exception e ) {
                        log.error( "Unable to cast date. ", e );
                        throw new RuntimeException( e );
                    }
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return literal.getValue2();
//                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    try {
                        Calendar daysSinceEpoch = (Calendar) literal.getValue();
                        return daysSinceEpoch.toInstant();
                    } catch ( Exception e ) {
                        log.error( "Unable to cast timestamp. ", e );
                        throw new RuntimeException( e );
                    }
                case BIGINT:
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
                    return (Long) literal.getValue2();
//                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return (Short) literal.getValue2();
//                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return (Byte) literal.getValue2();
//                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return (BigDecimal) literal.getValue();
//                    return BigDecimal.class;
                case BOOLEAN:
                    return (Boolean) literal.getValue2();
//                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                case FLOAT: // sic
                    return (Double) literal.getValue2();
//                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                    return (Float) literal.getValue2();
//                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return (ByteString) literal.getValue2();
//                    return ByteString.class;
                case GEOMETRY:
//                    return GeoFunctions.Geom.class;
                case SYMBOL:
//                    return Enum.class;
                case ANY:
                    return Object.class;
            }
        }
        return null;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( CassandraConvention.COST_MULTIPLIER );
    }


    @Override
    public void implement( CassandraImplementContext context ) {

        List<Map<String, Term>> items = new LinkedList<>();
        final List<RelDataTypeField> fields = rowType.getFieldList();
        for ( List<RexLiteral> tuple : tuples ) {
            final List<Expression> literals = new ArrayList<>();
            Map<String, Term> oneInsert = new LinkedHashMap<>();
            for ( Pair<RelDataTypeField, RexLiteral> pair : Pair.zip( fields, tuple ) ) {
                try {
                    oneInsert.put( pair.left.getName(), QueryBuilder.literal( literalValue( pair.right ) ) );
                } catch ( Exception e ) {
                    log.error( "Something broke while parsing cql values.", e );
                    throw new RuntimeException( e );
                }
            }

            items.add( oneInsert );
        }

        context.addInsertValues( items );
    }
}
