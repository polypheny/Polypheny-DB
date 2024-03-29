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


import java.util.Collection;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.util.Wrapper;


/**
 * Row expression.
 *
 * Every row-expression has a type. (Compare with {@link Node}, which is created before validation, and therefore types may not be available.)
 *
 * Some common row-expressions are: {@link RexLiteral} (constant value), {@link RexVariable} (variable), {@link RexCall} (call to operator with operands).
 * Expressions are generally created using a {@link RexBuilder} factory.
 *
 * All sub-classes of RexNode are immutable.
 */
public abstract class RexNode implements Wrapper {

    // Effectively final. Set in each sub-class constructor, and never re-set.
    protected String digest;


    public abstract AlgDataType getType();


    /**
     * Returns whether this expression always returns true. (Such as if this expression is equal to the literal <code>TRUE</code>.)
     */
    public boolean isAlwaysTrue() {
        return false;
    }


    /**
     * Returns whether this expression always returns false. (Such as if this expression is equal to the literal <code>FALSE</code>.)
     */
    public boolean isAlwaysFalse() {
        return false;
    }


    public boolean isA( Kind kind ) {
        return getKind() == kind;
    }


    public boolean isA( Collection<Kind> kinds ) {
        return getKind().belongsTo( kinds );
    }


    /**
     * Returns the kind of node this is.
     *
     * @return Node kind, never null
     */
    public Kind getKind() {
        return Kind.OTHER;
    }


    public String toString() {
        return digest;
    }


    /**
     * Accepts a visitor, dispatching to the right overloaded {@link RexVisitor#visitIndexRef visitXxx} method.
     *
     * Also see {@link RexUtil#apply(RexVisitor, java.util.List, RexNode)}, which applies a visitor to several expressions simultaneously.
     */
    public abstract <R> R accept( RexVisitor<R> visitor );

    /**
     * Accepts a visitor with a payload, dispatching to the right overloaded {@link RexBiVisitor#visitInputRef(RexIndexRef, Object)} visitXxx} method.
     */
    public abstract <R, P> R accept( RexBiVisitor<R, P> visitor, P arg );

    /**
     * {@inheritDoc}
     *
     * Every node must implement {@link #equals} based on its content
     */
    @Override
    public abstract boolean equals( Object obj );

    /**
     * {@inheritDoc}
     *
     * Every node must implement {@link #hashCode} consistent with {@link #equals}
     */
    @Override
    public abstract int hashCode();

}

