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

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;


/**
 * Sub-class of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort} not targeted at any particular engine or calling convention.
 */
public final class LogicalSort extends Sort {

    private LogicalSort( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, input, collation, offset, fetch );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalSort by parsing serialized output.
     */
    public LogicalSort( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalSort.
     *
     * @param input Input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    public static LogicalSort create( RelNode input, RelCollation collation, RexNode offset, RexNode fetch ) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize( collation );
        RelTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( collation );
        return new LogicalSort( cluster, traitSet, input, collation, offset, fetch );
    }


    @Override
    public Sort copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch ) {
        return new LogicalSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}

