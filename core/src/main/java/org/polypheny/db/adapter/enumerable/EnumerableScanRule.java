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

package org.polypheny.db.adapter.enumerable;


import java.util.function.Predicate;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalTableFunctionScan;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Table;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link LogicalTableFunctionScan} relational expression {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableScanRule extends ConverterRule {

    /**
     * Creates an EnumerableScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public EnumerableScanRule( AlgBuilderFactory algBuilderFactory ) {
        super( LogicalScan.class, (Predicate<AlgNode>) r -> true, Convention.NONE, EnumerableConvention.INSTANCE, algBuilderFactory, "EnumerableScanRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalScan scan = (LogicalScan) alg;
        final AlgOptTable algOptTable = scan.getTable();
        final Table table = algOptTable.unwrap( Table.class );
        if ( !EnumerableScan.canHandle( table ) ) {
            return null;
        }
        final Expression expression = algOptTable.getExpression( Object.class );
        if ( expression == null ) {
            return null;
        }
        return EnumerableScan.create( scan.getCluster(), algOptTable );
    }

}

