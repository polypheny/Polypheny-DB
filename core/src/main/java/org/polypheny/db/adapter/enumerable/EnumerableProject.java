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

package org.polypheny.db.adapter.enumerable;


import java.util.List;
import org.polypheny.db.core.ValidatorUtil;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.metadata.RelMdCollation;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;


/**
 * Implementation of {@link org.polypheny.db.rel.core.Project} in {@link org.polypheny.db.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 */
public class EnumerableProject extends Project implements EnumerableRel {

    /**
     * Creates an EnumerableProject.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param input Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType Output row type
     */
    public EnumerableProject( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert getConvention() instanceof EnumerableConvention;
    }


    /**
     * Creates an EnumerableProject, specifying row type rather than field names.
     */
    public static EnumerableProject create( final RelNode input, final List<? extends RexNode> projects, RelDataType rowType ) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet().replace( EnumerableConvention.INSTANCE )
                .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.project( mq, input, projects ) );
        return new EnumerableProject( cluster, traitSet, input, projects, rowType );
    }


    static RelNode create( RelNode child, List<? extends RexNode> projects, List<String> fieldNames ) {
        final RelOptCluster cluster = child.getCluster();
        final RelDataType rowType = RexUtil.createStructType( cluster.getTypeFactory(), projects, fieldNames, ValidatorUtil.F_SUGGESTER );
        return create( child, projects, rowType );
    }


    @Override
    public EnumerableProject copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
        return new EnumerableProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        // EnumerableCalcRel is always better
        throw new UnsupportedOperationException();
    }

}

