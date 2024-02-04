/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.polypheny.db.adapter.file.FileAlg.FileImplementor;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyValue;


/**
 * A value of an INSERT or UPDATE statement
 * The value can be fetched via the {@code getValue} method.
 * It comes from either a RexLiteral or from the dataContext parameterValues
 */
public class Value {

    @Getter
    @Setter
    private Integer columnReference;
    private PolyValue literal;
    private PolyValue literalIndex;


    /**
     * Value constructor
     *
     * @param columnReference May be null. Used by generated code, see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.algebra.FileToEnumerableConverter#implement}
     * @param literalOrIndex Either a literal or a literalIndex. The third parameter {@code isLiteralIndex} specifies if it is a literal or a literalIndex
     * @param isLiteralIndex True if the second parameter is a literalIndex. In this case, it has to be a Long
     */
    public Value( final Integer columnReference, final PolyValue literalOrIndex, final boolean isLiteralIndex ) {
        this.columnReference = columnReference;
        if ( isLiteralIndex ) {
            this.literalIndex = literalOrIndex;
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
    public PolyValue getValue( final DataContext context, final int i ) {
        //don't switch the two if conditions, because a literal assignment can be "null"
        if ( literalIndex != null ) {
            return context.getParameterValues().get( i ).get( literalIndex.asNumber().longValue() );
        } else {
            return literal;
        }
    }


    Expression getExpression() {
        if ( literal != null ) {
            return Expressions.new_( Value.class, Expressions.constant( columnReference ), literal.asExpression(), Expressions.constant( false ) );
        } else {
            return Expressions.new_( Value.class, Expressions.constant( columnReference ), literalIndex.asExpression(), Expressions.constant( true ) );
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
        if ( exps.size() == implementor.getFileTable().columns.size() ) {
            noCheck = true;
            offset = 0;
        } else {
            noCheck = false;
            offset = implementor.getFileTable().columns.size();
        }
        for ( int i = offset; i < implementor.getFileTable().columns.size() + offset; i++ ) {
            if ( noCheck || exps.size() > i ) {
                RexNode lit = exps.get( i );
                if ( lit instanceof RexLiteral literal ) {
                    valueList.add( new Value( null, literal.value, false ) );
                } else if ( lit instanceof RexDynamicParam dynamicParam ) {
                    valueList.add( new Value( null, PolyLong.of( dynamicParam.getIndex() ), true ) );
                } else if ( lit instanceof RexIndexRef indexRef ) {
                    valueList.add( new Value( indexRef.getIndex(), null, false ) );
                } else if ( lit instanceof RexCall call && lit.getType().getPolyType() == PolyType.ARRAY ) {
                    valueList.add( fromArrayRexCall( call ) );
                } else {
                    throw new GenericRuntimeException( "Could not implement " + lit.getClass().getSimpleName() + " " + lit );
                }
            }
        }
        return valueList;
    }


    public static Value fromArrayRexCall( final RexCall call ) {
        List<PolyValue> arrayValues = new ArrayList<>();
        for ( RexNode node : call.getOperands() ) {
            arrayValues.add( ((RexLiteral) node).value );
        }
        return new Value( null, PolyList.of( arrayValues ), false );
    }

}
