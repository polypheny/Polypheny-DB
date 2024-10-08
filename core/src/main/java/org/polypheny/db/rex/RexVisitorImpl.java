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


import java.util.List;


/**
 * Default implementation of {@link RexVisitor}, which visits each node but does nothing while it's there.
 *
 * @param <R> Return type from each {@code visitXxx} method.
 */
public class RexVisitorImpl<R> implements RexVisitor<R> {

    protected final boolean deep;


    protected RexVisitorImpl( boolean deep ) {
        this.deep = deep;
    }


    @Override
    public R visitIndexRef( RexIndexRef inputRef ) {
        return null;
    }


    @Override
    public R visitLocalRef( RexLocalRef localRef ) {
        return null;
    }


    @Override
    public R visitLiteral( RexLiteral literal ) {
        return null;
    }


    @Override
    public R visitOver( RexOver over ) {
        R r = visitCall( over );
        if ( !deep ) {
            return null;
        }
        final RexWindow window = over.getWindow();
        for ( RexFieldCollation orderKey : window.orderKeys ) {
            orderKey.left.accept( this );
        }
        for ( RexNode partitionKey : window.partitionKeys ) {
            partitionKey.accept( this );
        }
        window.getLowerBound().accept( this );
        window.getUpperBound().accept( this );
        return r;
    }


    @Override
    public R visitCorrelVariable( RexCorrelVariable correlVariable ) {
        return null;
    }


    @Override
    public R visitCall( RexCall call ) {
        if ( !deep ) {
            return null;
        }

        R r = null;
        for ( RexNode operand : call.operands ) {
            r = operand.accept( this );
        }
        return r;
    }


    @Override
    public R visitDynamicParam( RexDynamicParam dynamicParam ) {
        return null;
    }


    @Override
    public R visitRangeRef( RexRangeRef rangeRef ) {
        return null;
    }


    @Override
    public R visitFieldAccess( RexFieldAccess fieldAccess ) {
        if ( !deep ) {
            return null;
        }
        final RexNode expr = fieldAccess.getReferenceExpr();
        return expr.accept( this );
    }


    @Override
    public R visitSubQuery( RexSubQuery subQuery ) {
        if ( !deep ) {
            return null;
        }

        R r = null;
        for ( RexNode operand : subQuery.operands ) {
            r = operand.accept( this );
        }
        return r;
    }


    @Override
    public R visitTableInputRef( RexTableIndexRef ref ) {
        return null;
    }


    @Override
    public R visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
        return null;
    }


    @Override
    public R visitNameRef( RexNameRef nameRef ) {
        return null;
    }


    @Override
    public R visitElementRef( RexElementRef rexElementRef ) {
        return null;
    }


    /**
     * Visits an array of expressions, returning the logical 'and' of their results.
     *
     * If any of them returns false, returns false immediately; if they all return true, returns true.
     *
     * @see #visitArrayOr
     * @see RexShuttle#visitArray
     */
    public static boolean visitArrayAnd( RexVisitor<Boolean> visitor, List<RexNode> exprs ) {
        for ( RexNode expr : exprs ) {
            final boolean b = expr.accept( visitor );
            if ( !b ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Visits an array of expressions, returning the logical 'or' of their results.
     *
     * If any of them returns true, returns true immediately; if they all return false, returns false.
     *
     * @see #visitArrayAnd
     * @see RexShuttle#visitArray
     */
    public static boolean visitArrayOr( RexVisitor<Boolean> visitor, List<RexNode> exprs ) {
        for ( RexNode expr : exprs ) {
            final boolean b = expr.accept( visitor );
            if ( b ) {
                return true;
            }
        }
        return false;
    }

}
