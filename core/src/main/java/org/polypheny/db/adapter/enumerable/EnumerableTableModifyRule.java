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

package org.polypheny.db.adapter.enumerable;


import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link org.polypheny.db.algebra.logical.LogicalTableModify} relational expression {@link org.polypheny.db.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableTableModifyRule extends ConverterRule {

    /**
     * Creates an EnumerableTableModifyRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public EnumerableTableModifyRule( AlgBuilderFactory algBuilderFactory ) {
        super( LogicalTableModify.class, (Predicate<AlgNode>) r -> true, Convention.NONE, EnumerableConvention.INSTANCE, algBuilderFactory, "EnumerableTableModificationRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalTableModify modify = (LogicalTableModify) alg;
        if ( modify.isUpdate() ) {
            // this is something, which is not supported therefore we can just substitute it
            return EnumerableRules.ENUMERABLE_TABLE_MODIFY_TO_STREAMER_RULE.convert( alg );
        }

        final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
        if ( modifiableTable == null ) {
            return null;
        }
        final AlgTraitSet traitSet = modify.getTraitSet().replace( EnumerableConvention.INSTANCE );
        return new EnumerableTableModify(
                modify.getCluster(),
                traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert( modify.getInput(), traitSet ),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened() );
    }

}

