/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import java.util.Collection;


/**
 * Row expression.
 *
 * Every row-expression has a type. (Compare with {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode}, which is created before validation, and therefore types may not be available.)
 *
 * Some common row-expressions are: {@link RexLiteral} (constant value), {@link RexVariable} (variable), {@link RexCall} (call to operator with operands).
 * Expressions are generally created using a {@link RexBuilder} factory.
 *
 * All sub-classes of RexNode are immutable.
 */
public abstract class RexNode {

    // Effectively final. Set in each sub-class constructor, and never re-set.
    protected String digest;


    public abstract RelDataType getType();


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


    public boolean isA( SqlKind kind ) {
        return getKind() == kind;
    }


    public boolean isA( Collection<SqlKind> kinds ) {
        return getKind().belongsTo( kinds );
    }


    /**
     * Returns the kind of node this is.
     *
     * @return Node kind, never null
     */
    public SqlKind getKind() {
        return SqlKind.OTHER;
    }


    public String toString() {
        return digest;
    }


    /**
     * Accepts a visitor, dispatching to the right overloaded {@link RexVisitor#visitInputRef visitXxx} method.
     *
     * Also see {@link RexUtil#apply(RexVisitor, java.util.List, RexNode)}, which applies a visitor to several expressions simultaneously.
     */
    public abstract <R> R accept( RexVisitor<R> visitor );

    /**
     * Accepts a visitor with a payload, dispatching to the right overloaded {@link RexBiVisitor#visitInputRef(RexInputRef, Object)} visitXxx} method.
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

