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


import com.google.gson.Gson;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


/**
 * A value of an INSERT or UPDATE statement
 * The value can be fetched via the {@code getValue} method.
 * It comes from either a RexLiteral or from the dataContext parameterValues
 */
public class Value {

    @Getter
    @Setter
    private Integer columnReference;
    private Object literal;
    private Long literalIndex;
    private static final Gson gson = new Gson();


    /**
     * Value constructor
     *
     * @param columnReference May be null. Used by generated code, see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.algebra.FileToEnumerableConverter#implement}
     * @param literalOrIndex Either a literal or a literalIndex. The third parameter {@code isLiteralIndex} specifies if it is a literal or a literalIndex
     * @param isLiteralIndex True if the second parameter is a literalIndex. In this case, it has to be a Long
     */
    public Value( final Integer columnReference, final Object literalOrIndex, final boolean isLiteralIndex ) {
        this.columnReference = columnReference;
        if ( isLiteralIndex ) {
            this.literalIndex = (Long) literalOrIndex;
        } else {
            this.literal = literalOrIndex;
        }
    }


    /**
     * Get the value. It was either saved from a literal or is taken from the dataContext
     *
     * @param context Data context
     * @param i Index to retrieve the value from the ith parameterValues (needed for batch inserts)
     */
    public Object getValue( final DataContext context, final int i ) {
        //don't switch the two if conditions, because a literal assignment can be "null"
        if ( literalIndex != null ) {
            return context.getParameterValues().get( i ).get( literalIndex );
        } else {
            return literal;
        }
    }


    Expression getExpression() {
        if ( literal != null ) {
            Expression literalExpression;
            if ( literal instanceof Long ) {
                // this is a fix, else linq4j will submit an integer that is too long
                literalExpression = Expressions.constant( literal, Long.class );
            } else if ( literal instanceof BigDecimal ) {
                literalExpression = Expressions.constant( literal.toString() );
            } else {
                literalExpression = Expressions.constant( literal );
            }
            return Expressions.new_( Value.class, Expressions.constant( columnReference ), literalExpression, Expressions.constant( false ) );
        } else {
            return Expressions.new_( Value.class, Expressions.constant( columnReference ), Expressions.constant( literalIndex, Long.class ), Expressions.constant( true ) );
        }
    }


    public static Expression getValuesExpression( final List<Value> values ) {
        List<Expression> valueConstructors = new ArrayList<>();
        for ( Value value : values ) {
            valueConstructors.add( value.getExpression() );
        }
        return Expressions.newArrayInit( Value[].class, valueConstructors );
    }


    public static List<Value> getUpdates( final List<RexNode> exps, FileImplementor implementor ) {
        List<Value> valueList = new ArrayList<>();
        int offset;
        boolean noCheck;
        if ( exps.size() == implementor.getColumnNames().size() ) {
            noCheck = true;
            offset = 0;
        } else {
            noCheck = false;
            offset = implementor.getColumnNames().size();
        }
        for ( int i = offset; i < implementor.getColumnNames().size() + offset; i++ ) {
            if ( noCheck || exps.size() > i ) {
                RexNode lit = exps.get( i );
                if ( lit instanceof RexLiteral ) {
                    valueList.add( new Value( null, ((RexLiteral) lit).getValueForFileCondition(), false ) );
                } else if ( lit instanceof RexDynamicParam ) {
                    valueList.add( new Value( null, ((RexDynamicParam) lit).getIndex(), true ) );
                } else if ( lit instanceof RexInputRef ) {
                    valueList.add( new Value( ((RexInputRef) lit).getIndex(), null, false ) );
                } else if ( lit instanceof RexCall && lit.getType().getPolyType() == PolyType.ARRAY ) {
                    valueList.add( fromArrayRexCall( (RexCall) lit ) );
                } else {
                    throw new RuntimeException( "Could not implement " + lit.getClass().getSimpleName() + " " + lit.toString() );
                }
            }
        }
        return valueList;
    }


    public static Value fromArrayRexCall( final RexCall call ) {
        ArrayList<Object> arrayValues = new ArrayList<>();
        for ( RexNode node : call.getOperands() ) {
            arrayValues.add( ((RexLiteral) node).getValueForFileCondition() );
        }
        return new Value( null, gson.toJson( arrayValues ), false );
    }

}
