/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.interpreter;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.Window;
import org.polypheny.db.interpreter.Bindables.BindableScan;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.rex.RexNode;


/**
 * Helper methods for {@link Node} and implementations for core relational expressions.
 */
public class Nodes {

    /**
     * Extension to {@link Interpreter.CompilerImpl} that knows how to handle the core logical {@link AlgNode}s.
     */
    public static class CoreCompiler extends Interpreter.CompilerImpl {

        CoreCompiler( Interpreter interpreter, AlgOptCluster cluster ) {
            super( interpreter, cluster );
        }


        public void visit( Aggregate agg ) {
            node = new AggregateNode( this, agg );
        }


        public void visit( Filter filter ) {
            node = new FilterNode( this, filter );
        }


        public void visit( Project project ) {
            node = new ProjectNode( this, project );
        }


        public void visit( Values value ) {
            node = new ValuesNode( this, value );
        }


        public void visit( Scan scan ) {
            final ImmutableList<RexNode> filters = ImmutableList.of();
            node = ScanNode.create( this, scan, filters, null );
        }


        public void visit( BindableScan scan ) {
            node = ScanNode.create( this, scan, scan.filters, scan.projects );
        }


        public void visit( Sort sort ) {
            node = new SortNode( this, sort );
        }


        public void visit( Union union ) {
            node = new UnionNode( this, union );
        }


        public void visit( Join join ) {
            node = new JoinNode( this, join );
        }


        public void visit( Window window ) {
            node = new WindowNode( this, window );
        }

    }

}

