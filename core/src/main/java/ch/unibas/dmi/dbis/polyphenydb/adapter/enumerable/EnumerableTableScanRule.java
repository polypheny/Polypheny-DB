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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableFunctionScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Planner rule that converts a {@link LogicalTableFunctionScan} relational expression {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableTableScanRule extends ConverterRule {

    /**
     * Creates an EnumerableTableScanRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public EnumerableTableScanRule( RelBuilderFactory relBuilderFactory ) {
        super( LogicalTableScan.class, (Predicate<RelNode>) r -> true, Convention.NONE, EnumerableConvention.INSTANCE, relBuilderFactory, "EnumerableTableScanRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        LogicalTableScan scan = (LogicalTableScan) rel;
        final RelOptTable relOptTable = scan.getTable();
        final Table table = relOptTable.unwrap( Table.class );
        if ( !EnumerableTableScan.canHandle( table ) ) {
            return null;
        }
        final Expression expression = relOptTable.getExpression( Object.class );
        if ( expression == null ) {
            return null;
        }
        return EnumerableTableScan.create( scan.getCluster(), relOptTable );
    }
}

