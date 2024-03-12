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

package org.polypheny.db.algebra.mutable;


import com.google.common.base.Equivalence;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.util.Spaces;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;


/**
 * Mutable equivalent of {@link AlgNode}.
 *
 * Each node has mutable state, and keeps track of its parent and position within parent. It doesn't make sense to canonize {@code MutableRels},
 * otherwise one node could end up with multiple parents. It follows that {@code #hashCode} and {@code #equals} are less efficient
 * than their {@code AlgNode} counterparts. But, you don't need to copy a {@code MutableRel} in order to change it.
 * For this reason, you should use {@code MutableRel} for short-lived operations, and transcribe back to {@code AlgNode} when you are done.
 */
public abstract class MutableAlg {

    /**
     * Equivalence that compares objects by their {@link Object#toString()} method.
     */
    protected static final Equivalence<Object> STRING_EQUIVALENCE =
            new Equivalence<Object>() {
                @Override
                protected boolean doEquivalent( Object o, Object o2 ) {
                    return o.toString().equals( o2.toString() );
                }


                @Override
                protected int doHash( Object o ) {
                    return o.toString().hashCode();
                }
            };

    /**
     * Equivalence that compares {@link Lists}s by the {@link Object#toString()} of their elements.
     */
    @SuppressWarnings("unchecked")
    protected static final Equivalence<List<?>> PAIRWISE_STRING_EQUIVALENCE = (Equivalence) STRING_EQUIVALENCE.pairwise();

    public final AlgCluster cluster;
    public final AlgDataType rowType;
    protected final MutableAlgType type;

    protected MutableAlg parent;
    protected int ordinalInParent;


    protected MutableAlg( AlgCluster cluster, AlgDataType rowType, MutableAlgType type ) {
        this.cluster = Objects.requireNonNull( cluster );
        this.rowType = Objects.requireNonNull( rowType );
        this.type = Objects.requireNonNull( type );
    }


    public MutableAlg getParent() {
        return parent;
    }


    public abstract void setInput( int ordinalInParent, MutableAlg input );

    public abstract List<MutableAlg> getInputs();

    @Override
    public abstract MutableAlg clone();

    public abstract void childrenAccept( MutableAlgVisitor visitor );


    /**
     * Replaces this {@code MutableRel} in its parent with another node at the same position.
     *
     * Before the method, {@code child} must be an orphan (have null parent) and after this method, this {@code MutableRel} is an orphan.
     *
     * @return The parent
     */
    public MutableAlg replaceInParent( MutableAlg child ) {
        final MutableAlg parent = this.parent;
        if ( this != child ) {
            if ( parent != null ) {
                parent.setInput( ordinalInParent, child );
                this.parent = null;
                this.ordinalInParent = 0;
            }
        }
        return parent;
    }


    public abstract StringBuilder digest( StringBuilder buf );


    public final String deep() {
        return new MutableRelDumper().apply( this );
    }


    @Override
    public final String toString() {
        return deep();
    }


    /**
     * Implementation of MutableVisitor that dumps the details of a MutableRel tree.
     */
    private class MutableRelDumper extends MutableAlgVisitor {

        private final StringBuilder buf = new StringBuilder();
        private int level;


        @Override
        public void visit( MutableAlg node ) {
            Spaces.append( buf, level * 2 );
            if ( node == null ) {
                buf.append( "null" );
            } else {
                node.digest( buf );
                buf.append( "\n" );
                ++level;
                super.visit( node );
                --level;
            }
        }


        public String apply( MutableAlg alg ) {
            go( alg );
            return buf.toString();
        }

    }

}

