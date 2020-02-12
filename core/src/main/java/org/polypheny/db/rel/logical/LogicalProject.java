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
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import java.util.List;


/**
 * Sub-class of {@link Project} not targeted at any particular engine or calling convention.
 */
public final class LogicalProject extends Project {

    /**
     * Creates a LogicalProject.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param input Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType Output row type
     */
    public LogicalProject( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalProject by parsing serialized output.
     */
    public LogicalProject( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalProject.
     */
    public static LogicalProject create( final RelNode input, final List<? extends RexNode> projects, List<String> fieldNames ) {
        final RelOptCluster cluster = input.getCluster();
        final RelDataType rowType = RexUtil.createStructType( cluster.getTypeFactory(), projects, fieldNames, SqlValidatorUtil.F_SUGGESTER );
        return create( input, projects, rowType );
    }


    /**
     * Creates a LogicalProject, specifying row type rather than field names.
     */
    public static LogicalProject create( final RelNode input, final List<? extends RexNode> projects, RelDataType rowType ) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet()
                .replace( Convention.NONE )
                .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.project( mq, input, projects ) );
        return new LogicalProject( cluster, traitSet, input, projects, rowType );
    }


    @Override
    public LogicalProject copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
        return new LogicalProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}

