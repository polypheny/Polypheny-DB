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

package org.polypheny.db.algebra.core;


import com.google.common.base.Preconditions;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.*;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyTypeUtil;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Relational expression that executes a trigger.
 *
 */
public abstract class SingleTriggerExecution extends SingleAlg {

    /**
     * The table definition.
     */
    protected final AlgOptTable table;


    protected SingleTriggerExecution( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table ) {
        super( cluster, traitSet, null );
        this.table = table;
    }

    /**
     * Creates a TableScan by parsing serialized output.
     */
    protected SingleTriggerExecution( AlgInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getTable( "table" ) );
    }

    @Override
    public AlgOptTable getTable() {
        return table;
    }

//    @Override
//    public AlgDataType deriveRowType() {
//        return AlgOptUtil.createDmlRowType( Kind.INSERT, getCluster().getTypeFactory() );
//    }
//
//    @Override
//    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
//        double rowCount = mq.getRowCount( this );
//        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
//    }

}

