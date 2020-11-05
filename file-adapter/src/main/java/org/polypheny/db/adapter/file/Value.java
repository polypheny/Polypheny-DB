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


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


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


    /**
     * Value constructor
     *
     * @param columnReference May be null. Used by generated code, see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
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


    public Object getValue( final DataContext context ) {
        //don't switch the two if conditions, because a literal assignment can be "null"
        if ( literalIndex != null ) {
            return context.getParameterValue( literalIndex );
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
        int offset = implementor.getColumnNames().size();
        for ( int i = 0; i < offset; i++ ) {
            if ( exps.size() > i + offset ) {
                RexNode lit = exps.get( i + offset );
                if ( lit instanceof RexLiteral ) {
                    valueList.add( new Value( null, ((RexLiteral) lit).getValueForFileCondition(), false ) );
                } else {
                    valueList.add( new Value( null, ((RexDynamicParam) lit).getIndex(), true ) );
                }
            }
        }
        return valueList;
    }
}
