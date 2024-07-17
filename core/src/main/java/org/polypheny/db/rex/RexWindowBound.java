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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.rex;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Window;


/**
 * Abstracts "XX PRECEDING/FOLLOWING" and "CURRENT ROW" bounds for windowed aggregates.
 */
public abstract class RexWindowBound {

    /**
     * Creates window bound.
     *
     * @param node SqlNode of the bound
     * @param rexNode offset value when bound is not UNBOUNDED/CURRENT ROW
     * @return window bound
     */
    public static RexWindowBound create( Node node, RexNode rexNode ) {
        if ( Window.isUnboundedPreceding( node ) || Window.isUnboundedFollowing( node ) ) {
            return new RexWindowBoundUnbounded( node );
        }
        if ( Window.isCurrentRow( node ) ) {
            return new RexWindowBoundCurrentRow();
        }
        return new RexWindowBoundBounded( rexNode );
    }


    /**
     * Returns if the bound is unbounded.
     *
     * @return if the bound is unbounded
     */
    public boolean isUnbounded() {
        return false;
    }


    /**
     * Returns if the bound is PRECEDING.
     *
     * @return if the bound is PRECEDING
     */
    public boolean isPreceding() {
        return false;
    }


    /**
     * Returns if the bound is FOLLOWING.
     *
     * @return if the bound is FOLLOWING
     */
    public boolean isFollowing() {
        return false;
    }


    /**
     * Returns if the bound is CURRENT ROW.
     *
     * @return if the bound is CURRENT ROW
     */
    public boolean isCurrentRow() {
        return false;
    }


    /**
     * Returns offset from XX PRECEDING/FOLLOWING.
     *
     * @return offset from XX PRECEDING/FOLLOWING
     */
    public RexNode getOffset() {
        return null;
    }


    /**
     * Returns relative sort offset when known at compile time.
     * For instance, UNBOUNDED PRECEDING is less than CURRENT ROW.
     *
     * @return relative order or -1 when order is not known
     */
    public int getOrderKey() {
        return -1;
    }


    /**
     * Transforms the bound via {@link org.polypheny.db.rex.RexVisitor}.
     *
     * @param visitor visitor to accept
     * @param <R> return type of the visitor
     * @return transformed bound
     */
    public <R> RexWindowBound accept( RexVisitor<R> visitor ) {
        return this;
    }


    /**
     * Returns a string representation of the bound using the given visitor to transform
     * any RexNode in the process to a string.
     *
     * @param visitor the RexVisitor used to transform RexNodes into strings
     * @return String representation of this bound
     */
    public String toString( RexVisitor<String> visitor ) {
        return toString();
    }


    /**
     * Implements UNBOUNDED PRECEDING/FOLLOWING bound.
     */
    private static class RexWindowBoundUnbounded extends RexWindowBound {

        private final Node node;


        RexWindowBoundUnbounded( Node node ) {
            this.node = node;
        }


        @Override
        public boolean isUnbounded() {
            return true;
        }


        @Override
        public boolean isPreceding() {
            return Window.isUnboundedPreceding( node );
        }


        @Override
        public boolean isFollowing() {
            return Window.isUnboundedFollowing( node );
        }


        @Override
        public String toString() {
            return ((Literal) node).getValue().toString();
        }


        @Override
        public int getOrderKey() {
            return isPreceding() ? 0 : 2;
        }


        @Override
        public boolean equals( Object o ) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }

            RexWindowBoundUnbounded that = (RexWindowBoundUnbounded) o;

            if ( !node.equals( that.node ) ) {
                return false;
            }

            return true;
        }


        @Override
        public int hashCode() {
            return node.hashCode();
        }

    }


    /**
     * Implements CURRENT ROW bound.
     */
    private static class RexWindowBoundCurrentRow extends RexWindowBound {

        @Override
        public boolean isCurrentRow() {
            return true;
        }


        @Override
        public String toString() {
            return "CURRENT ROW";
        }


        @Override
        public int getOrderKey() {
            return 1;
        }


        @Override
        public boolean equals( Object obj ) {
            return getClass() == obj.getClass();
        }


        @Override
        public int hashCode() {
            return 123;
        }

    }


    /**
     * Implements XX PRECEDING/FOLLOWING bound where XX is not UNBOUNDED.
     */
    private static class RexWindowBoundBounded extends RexWindowBound {

        private final Kind kind;
        private final RexNode offset;


        RexWindowBoundBounded( RexNode node ) {
            assert node instanceof RexCall : "RexWindowBoundBounded window bound should be either 'X preceding'" + " or 'X following' call. Actual type is " + node;
            RexCall call = (RexCall) node;
            this.offset = call.getOperands().get( 0 );
            this.kind = call.getKind();
            assert this.offset != null : "RexWindowBoundBounded offset should not be null";
        }


        private RexWindowBoundBounded( Kind kind, RexNode offset ) {
            this.kind = kind;
            this.offset = offset;
        }


        @Override
        public boolean isPreceding() {
            return kind == Kind.PRECEDING;
        }


        @Override
        public boolean isFollowing() {
            return kind == Kind.FOLLOWING;
        }


        @Override
        public RexNode getOffset() {
            return offset;
        }


        @Override
        public <R> RexWindowBound accept( RexVisitor<R> visitor ) {
            R r = offset.accept( visitor );
            if ( r instanceof RexNode && r != offset ) {
                return new RexWindowBoundBounded( kind, (RexNode) r );
            }
            return this;
        }


        @Override
        public String toString() {
            return offset.toString() + " " + kind.toString();
        }


        @Override
        public String toString( RexVisitor<String> visitor ) {
            if ( visitor == null ) {
                return toString();
            }
            return offset.accept( visitor ) + " " + kind.toString();
        }


        @Override
        public boolean equals( Object o ) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }

            RexWindowBoundBounded that = (RexWindowBoundBounded) o;

            if ( !offset.equals( that.offset ) ) {
                return false;
            }
            if ( kind != that.kind ) {
                return false;
            }

            return true;
        }


        @Override
        public int hashCode() {
            int result = kind.hashCode();
            result = 31 * result + offset.hashCode();
            return result;
        }

    }

}


