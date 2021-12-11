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

package org.polypheny.db.nodes;


/**
 * Basic implementation of {@link NodeVisitor} which does nothing at each node.
 *
 * This class is useful as a base class for classes which implement the {@link NodeVisitor} interface. The derived class can override whichever methods it chooses.
 *
 * @param <R> Return type
 */
public class BasicNodeVisitor<R> implements NodeVisitor<R> {

    @Override
    public R visit( Literal literal ) {
        return null;
    }


    @Override
    public R visit( Call call ) {
        return call.getOperator().acceptCall( this, call );
    }


    @Override
    public R visit( NodeList nodeList ) {
        R result = null;
        for ( int i = 0; i < nodeList.size(); i++ ) {
            Node node = nodeList.get( i );
            result = node.accept( this );
        }
        return result;
    }


    @Override
    public R visit( Identifier id ) {
        return null;
    }


    @Override
    public R visit( DataTypeSpec type ) {
        return null;
    }


    @Override
    public R visit( DynamicParam param ) {
        return null;
    }


    @Override
    public R visit( IntervalQualifier intervalQualifier ) {
        return null;
    }


    /**
     * Argument handler.
     *
     * @param <R> result type
     */
    public interface ArgHandler<R> {

        /**
         * Returns the result of visiting all children of a call to an operator, then the call itself.
         *
         * Typically the result will be the result of the last child visited, or (if R is {@link Boolean}) whether all children were visited successfully.
         */
        R result();

        /**
         * Visits a particular operand of a call, using a given visitor.
         */
        R visitChild( NodeVisitor<R> visitor, Node expr, int i, Node operand );

    }


    /**
     * Default implementation of {@link ArgHandler} which merely calls {@link Node#accept} on each operand.
     *
     * @param <R> result type
     */
    public static class ArgHandlerImpl<R> implements ArgHandler<R> {

        private static final ArgHandler INSTANCE = new ArgHandlerImpl();


        @SuppressWarnings("unchecked")
        public static <R> ArgHandler<R> instance() {
            return INSTANCE;
        }


        @Override
        public R result() {
            return null;
        }


        @Override
        public R visitChild( NodeVisitor<R> visitor, Node expr, int i, Node operand ) {
            if ( operand == null ) {
                return null;
            }
            return operand.accept( visitor );
        }

    }

}

