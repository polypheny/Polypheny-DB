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

package org.polypheny.db.adapter.file;


import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;


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

    private void assignRexNode( final RexNode rexNode ) {
        if ( rexNode instanceof RexInputRef ) {
            this.columnReference = ((RexInputRef) rexNode).getIndex();
        } else if ( rexNode instanceof RexDynamicParam ) {
            this.literalIndex = ((RexDynamicParam) rexNode).getIndex();
        } else if ( rexNode instanceof RexLiteral ) {
            this.literal = ((RexLiteral) rexNode).getValueForFileCondition();
        }
    }

    //todo add column types
    public boolean matches( final Comparable[] columnValues, final DataContext dataContext ) {
        if ( columnReference == null ) { // || literalIndex == null ) {
            switch ( operator ) {
                case AND:
                    for ( Condition c : operands ) {
                        if ( !c.matches( columnValues, dataContext ) ) {
                            return false;
                        }
                    }
                    return true;
                case OR:
                    for ( Condition c : operands ) {
                        if ( c.matches( columnValues, dataContext ) ) {
                            return true;
                        }
                    }
                    return false;
                default:
                    throw new RuntimeException( operator + " not supported in condition without columnReference" );
            }
        }
        Comparable columnValue = columnValues[columnReference];//don't do the projectionMapping here
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
        Object parameterValue;
        if ( this.literalIndex != null ) {
            parameterValue = dataContext.getParameterValue( literalIndex );
        } else {
            parameterValue = this.literal;
        }
        if ( parameterValue == null ) {
            //WHERE x = null is always false, see https://stackoverflow.com/questions/9581745/sql-is-null-and-null
            return false;
        }
        if ( columnValue instanceof Number && parameterValue instanceof Number ) {
            columnValue = ((Number) columnValue).doubleValue();
            parameterValue = ((Number) parameterValue).doubleValue();
        }

        int comparison;

        //Todo at the moment the comparison is performed with a string comparison, and it actually works.
        // But it would be safer to convert both strings to calendars and compare them
        /*if ( parameterValue instanceof Calendar ) {
            Calendar cal;
            switch ( columnValue.toString().length() ) {
                case 10://Date
                    cal = DateTimeUtils.parseDateFormat( columnValue.toString(), new SimpleDateFormat( DateTimeUtils.DATE_FORMAT_STRING ), DateTimeUtils.UTC_ZONE );
                    comparison = ((GregorianCalendar) cal).toZonedDateTime().toLocalDate().compareTo( ((GregorianCalendar) parameterValue).toZonedDateTime().toLocalDate() );
                    break;
                case 8://time
                    cal = DateTimeUtils.parseDateFormat( columnValue.toString(), new SimpleDateFormat( DateTimeUtils.TIME_FORMAT_STRING ), DateTimeUtils.UTC_ZONE );
                    comparison = ((GregorianCalendar) cal).toZonedDateTime().toLocalTime().compareTo( ((GregorianCalendar) parameterValue).toZonedDateTime().toLocalTime() );
                    break;
                case 19://timestamp
                    cal = DateTimeUtils.parseDateFormat( columnValue.toString(), new SimpleDateFormat( DateTimeUtils.TIMESTAMP_FORMAT_STRING ), DateTimeUtils.UTC_ZONE );
                    comparison = ((GregorianCalendar) cal).toZonedDateTime().toLocalDateTime().compareTo( ((GregorianCalendar) parameterValue).toZonedDateTime().toLocalDateTime() );
                    break;
                default:
                    throw new RuntimeException( "Date/Time/Timestamp String in unexpected Format: " + columnValue.toString() );
            }
        } else {
            comparison = columnValue.compareTo( parameterValue );
        }*/
        comparison = columnValue.compareTo( parameterValue );

        switch ( operator ) {
            case AND:
                for ( Condition c : operands ) {
                    if ( !c.matches( columnValues, dataContext ) ) {
                        return false;
                    }
                }
                return true;
            case OR:
                for ( Condition c : operands ) {
                    if ( c.matches( columnValues, dataContext ) ) {
                        return true;
                    }
                }
                return false;
            case EQUALS:
                return comparison == 0;
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

    public String toJson() {
        return new Gson().toJson( this, Condition.class );
    }

    public static Condition fromJson( final String serializedCondition ) {
        return new Gson().fromJson( serializedCondition, Condition.class );
    }
}
