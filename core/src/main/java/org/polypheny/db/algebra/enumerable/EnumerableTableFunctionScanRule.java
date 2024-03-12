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

package org.polypheny.db.algebra.enumerable;


import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link LogicalRelTableFunctionScan} relational expression {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableTableFunctionScanRule extends ConverterRule {

    /**
     * Creates an EnumerableTableFunctionScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public EnumerableTableFunctionScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                LogicalRelTableFunctionScan.class,
                (Predicate<AlgNode>) r -> true,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                "EnumerableTableFunctionScanRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final AlgTraitSet traitSet = alg.getTraitSet().replace( EnumerableConvention.INSTANCE );
        LogicalRelTableFunctionScan tbl = (LogicalRelTableFunctionScan) alg;
        return new EnumerableRelTableFunctionScan(
                alg.getCluster(),
                traitSet,
                tbl.getInputs(),
                tbl.getElementType(),
                tbl.getTupleType(),
                tbl.getCall(),
                tbl.getColumnMappings() );
    }

}

