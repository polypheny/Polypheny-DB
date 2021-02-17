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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.type.PolyType;


public class Condition {

    private final SqlKind operator;
    private Integer columnReference;
    private Long literalIndex;
    private Object literal;
    private ArrayList<Condition> operands = new ArrayList<>();


    public Condition( final RexCall call ) {
        this.operator = call.getOperator().getKind();
        for ( RexNode rex : call.getOperands() ) {
            if ( rex instanceof RexCall ) {
                this.operands.add( new Condition( (RexCall) rex ) );
            } else {
                RexNode n0 = call.getOperands().get( 0 );
                assignRexNode( n0 );
                if ( call.getOperands().size() > 1 ) { // IS NULL and IS NOT NULL have no literal/literalIndex
                    RexNode n1 = call.getOperands().get( 1 );
                    assignRexNode( n1 );
                }
            }
        }
    }


    /**
     * Called by generated code, see {@link Condition#getExpression}
     */
    public Condition( final SqlKind operator, final Integer columnReference, final Long literalIndex, final Object literal, final Condition[] operands ) {
        this.operator = operator;
        this.columnReference = columnReference;
        this.literalIndex = literalIndex;
        this.literal = literal;
        this.operands.addAll( Arrays.asList( operands.clone() ) );
    }


    /**
     * For linq4j Expressions
     */
    public Expression getExpression() {
        List<Expression> operandsExpressions = new ArrayList<>();
        for ( Condition operand : operands ) {
            operandsExpressions.add( operand.getExpression() );
        }

        return Expressions.new_(
                Condition.class,
                Expressions.constant( operator, SqlKind.class ),
                Expressions.constant( columnReference, Integer.class ),
                Expressions.constant( literalIndex, Long.class ),
                Expressions.constant( this.literal ),
                Expressions.newArrayInit( Condition.class, operandsExpressions )
        );
    }


    private void assignRexNode( final RexNode rexNode ) {
        if ( rexNode instanceof RexInputRef ) {
            this.columnReference = ((RexInputRef) rexNode).getIndex();
        } else if ( rexNode instanceof RexDynamicParam ) {
            this.literalIndex = ((RexDynamicParam) rexNode).getIndex();
        } else if ( rexNode instanceof RexLiteral ) {
            this.literal = ((RexLiteral) rexNode).getValueForFileCondition();
        }
    }


    /**
     * Determines if a condition is a primary key condition, i.e. an AND-condition over all primary key columns
     *
     * @param pkColumnReferences One-based references of the PK columns, e.g. [1,3] for [a,b,c] if a and c are the primary key columns
     * @param colSize Number of columns in the current query, needed to generate the object that will be hashed
     * @return {@code Null} if it is not a PK lookup, or an Object array with the lookups to hash, if it is a PK lookup
     */
    @Nullable
    public Object getPKLookup( final Set<Integer> pkColumnReferences, final PolyType[] columnTypes, final int colSize, final DataContext dataContext ) {
        Object[] lookups = new Object[colSize];
        if ( operator == SqlKind.EQUALS && pkColumnReferences.size() == 1 ) {
            if ( pkColumnReferences.contains( columnReference ) ) {
                lookups[columnReference] = getParamValue( dataContext, columnTypes[columnReference] );
                return lookups;
            } else {
                return null;
            }
        } else if ( operator == SqlKind.AND ) {
            for ( Condition operand : operands ) {
                if ( operand.operator == SqlKind.EQUALS ) {
                    if ( !pkColumnReferences.contains( operand.columnReference ) ) {
                        return null;
                    } else {
                        pkColumnReferences.remove( operand.columnReference );
                    }
                } else {
                    return null;
                }
            }
            if ( pkColumnReferences.size() == 0 ) {
                return lookups;
            } else {
                return null;
            }
        }
        return null;
    }


    /**
     * Get the value of the condition parameter, either from the literal or literalIndex
     */
    Object getParamValue( final DataContext dataContext, final PolyType polyType ) {
        Object out;
        if ( this.literalIndex != null ) {
            out = dataContext.getParameterValue( literalIndex );
        } else {
            out = this.literal;
        }
        if ( out instanceof Calendar ) {
            switch ( polyType ) {
                case TIME:
                case TIMESTAMP:
                    return ((Calendar) out).getTimeInMillis();
                case DATE:
                    Calendar cal = ((Calendar) out);
                    return LocalDateTime.ofInstant( cal.toInstant(), cal.getTimeZone().toZoneId() ).toLocalDate().toEpochDay();
            }
        }
        return out;
    }


    public boolean matches( final Object[] columnValues, final PolyType[] columnTypes, final DataContext dataContext ) {
        if ( columnReference == null ) { // || literalIndex == null ) {
            switch ( operator ) {
                case AND:
                    for ( Condition c : operands ) {
                        if ( !c.matches( columnValues, columnTypes, dataContext ) ) {
                            return false;
                        }
                    }
                    return true;
                case OR:
                    for ( Condition c : operands ) {
                        if ( c.matches( columnValues, columnTypes, dataContext ) ) {
                            return true;
                        }
                    }
                    return false;
                default:
                    throw new RuntimeException( operator + " not supported in condition without columnReference" );
            }
        }
        // don't allow comparison of files and return false if Objects are not comparable
        if ( columnValues[columnReference] != null && (!(columnValues[columnReference] instanceof Comparable) || (columnValues[columnReference] instanceof File)) ) {
            return false;
        }
        Comparable columnValue = (Comparable) columnValues[columnReference];//don't do the projectionMapping here
        PolyType polyType = columnTypes[columnReference];
        switch ( operator ) {
            case IS_NULL:
                return columnValue == null;
            case IS_NOT_NULL:
                return columnValue != null;
        }
        if ( columnValue == null ) {
            //if there is no null check and the column value is null, any check on the column value would return false
            return false;
        }
        Object parameterValue = getParamValue( dataContext, polyType );
        if ( parameterValue == null ) {
            //WHERE x = null is always false, see https://stackoverflow.com/questions/9581745/sql-is-null-and-null
            return false;
        }
        if ( columnValue instanceof Number && parameterValue instanceof Number ) {
            columnValue = ((Number) columnValue).doubleValue();
            parameterValue = ((Number) parameterValue).doubleValue();
        }

        int comparison;

        if ( parameterValue instanceof Calendar ) {
            //could be improved with precision..
            switch ( polyType ) {
                case DATE:
                    LocalDate ld = LocalDate.ofEpochDay( (Integer) columnValue );
                    comparison = ld.compareTo( ((GregorianCalendar) parameterValue).toZonedDateTime().toLocalDate() );
                    break;
                case TIME:
                    //see https://howtoprogram.xyz/2017/02/11/convert-milliseconds-localdatetime-java/
                    LocalTime dt = Instant.ofEpochMilli( (Integer) columnValue ).atZone( DateTimeUtils.UTC_ZONE.toZoneId() ).toLocalTime();
                    comparison = dt.compareTo( ((GregorianCalendar) parameterValue).toZonedDateTime().toLocalTime() );
                    break;
                case TIMESTAMP:
                    LocalDateTime ldt = Instant.ofEpochMilli( (Long) columnValue ).atZone( DateTimeUtils.UTC_ZONE.toZoneId() ).toLocalDateTime();
                    comparison = ldt.compareTo( ((GregorianCalendar) parameterValue).toZonedDateTime().toLocalDateTime() );
                    break;
                default:
                    comparison = columnValue.compareTo( parameterValue );
            }
        } else if ( FileHelper.isSqlDateOrTimeOrTS( parameterValue ) ) {
            switch ( polyType ) {
                case TIME:
                case DATE:
                    comparison = Long.valueOf( (Integer) columnValue ).compareTo( FileHelper.sqlToLong( parameterValue ) );
                    break;
                case TIMESTAMP:
                default:
                    comparison = columnValue.compareTo( FileHelper.sqlToLong( parameterValue ) );
            }
        } else {
            comparison = columnValue.compareTo( parameterValue );
        }

        switch ( operator ) {
            case AND:
                for ( Condition c : operands ) {
                    if ( !c.matches( columnValues, columnTypes, dataContext ) ) {
                        return false;
                    }
                }
                return true;
            case OR:
                for ( Condition c : operands ) {
                    if ( c.matches( columnValues, columnTypes, dataContext ) ) {
                        return true;
                    }
                }
                return false;
            case EQUALS:
                return comparison == 0;
            case NOT_EQUALS:
                return comparison != 0;
            case GREATER_THAN:
                return comparison > 0;
            case GREATER_THAN_OR_EQUAL:
                return comparison >= 0;
            case LESS_THAN:
                return comparison < 0;
            case LESS_THAN_OR_EQUAL:
                return comparison <= 0;
            case LIKE:
                //todo maybe replace '%' by '(.*)' etc.
                Pattern pattern = Pattern.compile( parameterValue.toString() );
                Matcher matcher = pattern.matcher( columnValue.toString() );
                return matcher.matches();
            default:
                throw new RuntimeException( operator + " comparison not supported by file adapter." );
        }
    }

}
