/*
 * Copyright 2019-2024 The Polypheny Project
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


import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;


/**
 * A value of an INSERT or UPDATE statement
 * The value can be fetched via the {@code getValue} method.
 * It comes from either a RexLiteral or from the dataContext parameterValues
 */
public abstract class Value extends PolyValue {

    @Getter
    @Setter
    Integer columnReference;

    public final ValueType valueType;


    /**
     * Value constructor
     *
     * @param columnReference May be null. Used by generated code, see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.algebra.FileToEnumerableConverter#implement}
     */
    public Value( final ValueType valueType, final Integer columnReference ) {
        super( PolyType.FILE );
        this.columnReference = columnReference;
        this.valueType = valueType;
    }


    /**
     * Get the value. It was either saved from a literal or is taken from the dataContext
     *
     * @param context Data context
     * @param batchIndex Index to retrieve the value from the ith parameterValues (needed for batch inserts)
     */
    public abstract PolyValue getValue( List<PolyValue> values, final DataContext context, final int batchIndex );

    public abstract void adjust( List<Value> projectionMapping );


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return null;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return 0;
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), Value.class );
    }


    @Getter
    public static class DynamicValue extends Value {

        private final long index;


        public DynamicValue( final Integer columnReference, final long index ) {
            super( ValueType.DYNAMIC, columnReference );
            this.index = index;
        }


        /**
         * Get the value. It was either saved from a literal or is taken from the dataContext
         *
         * @param context Data context
         * @param batchIndex Index to retrieve the value from the ith parameterValues (needed for batch inserts)
         */
        public PolyValue getValue( final List<PolyValue> values, final DataContext context, final int batchIndex ) {
            //don't switch the two if conditions, because a literal assignment can be "null"
            return context.getParameterValues().get( batchIndex ).get( index );
        }


        @Override
        public void adjust( List<Value> projectionMapping ) {
            // nothing to do
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( DynamicValue.class, Expressions.constant( columnReference ), Expressions.constant( index ) );
        }

    }


    @Getter
    public static class LiteralValue extends Value {

        @Nullable
        private final PolyValue literal;


        public LiteralValue( final Integer columnReference, @Nullable PolyValue literal ) {
            super( ValueType.LITERAL, columnReference );
            this.literal = literal;
        }


        /**
         * Get the value. It was either saved from a literal or is taken from the dataContext
         *
         * @param context Data context
         * @param batchIndex Index to retrieve the value from the ith parameterValues (needed for batch inserts)
         */
        public PolyValue getValue( final List<PolyValue> values, final DataContext context, final int batchIndex ) {
            //don't switch the two if conditions, because a literal assignment can be "null"
            return literal;
        }


        @Override
        public void adjust( List<Value> projectionMapping ) {
            // nothing to do
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( LiteralValue.class, Expressions.constant( columnReference ), literal == null ? Expressions.constant( null ) : literal.asExpression() );
        }

    }


    @Getter
    public static class InputValue extends Value {

        private int index;


        public InputValue( final Integer columnReference, final int index ) {
            super( ValueType.INPUT, columnReference );
            this.index = index;
        }


        /**
         * Get the value. It was either saved from a literal or is taken from the dataContext
         *
         * @param context Data context
         * @param batchIndex Index to retrieve the value from the ith parameterValues (needed for batch inserts)
         */
        public PolyValue getValue( final List<PolyValue> values, final DataContext context, final int batchIndex ) {
            return values.get( index );
        }


        @Override
        public void adjust( List<Value> projectionMapping ) {
            index = ((InputValue) projectionMapping.get( index )).getIndex();
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_( InputValue.class, Expressions.constant( columnReference ), Expressions.constant( index ) );
        }

    }


    public enum ValueType {
        DYNAMIC,
        LITERAL,
        INPUT
    }

}
