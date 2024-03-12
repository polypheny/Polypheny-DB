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


import java.util.List;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Project} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableProject extends Project implements EnumerableAlg {

    /**
     * Creates an EnumerableProject.
     * <p>
     * Use create unless you know what you're doing.
     *
     * @param cluster Cluster this algebra expression belongs to
     * @param traitSet Traits of this algebra expression
     * @param input Input algebra expression
     * @param projects List of expressions for the input columns
     * @param rowType Output row type
     */
    public EnumerableProject( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert getConvention() instanceof EnumerableConvention;
    }


    /**
     * Creates an EnumerableProject, specifying row type rather than field names.
     */
    public static EnumerableProject create( final AlgNode input, final List<? extends RexNode> projects, AlgDataType rowType ) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = input.getTraitSet().replace( EnumerableConvention.INSTANCE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.project( mq, input, projects ) );
        return new EnumerableProject( cluster, traitSet, input, projects, rowType );
    }


    static AlgNode create( AlgNode child, List<? extends RexNode> projects, List<String> fieldNames ) {
        final AlgCluster cluster = child.getCluster();
        final AlgDataType rowType = RexUtil.createStructType( cluster.getTypeFactory(), projects, fieldNames, ValidatorUtil.F_SUGGESTER );
        return create( child, projects, rowType );
    }


    @Override
    public EnumerableProject copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new EnumerableProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return planner.getCostFactory().makeInfiniteCost();
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        // EnumerableCalcRel is always better
        throw new UnsupportedOperationException();
    }

}

