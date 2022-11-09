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

package org.polypheny.db.algebra.logical.relational;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Subclass of {@link Project} not targeted at any particular engine or calling convention.
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
    public LogicalProject( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalProject by parsing serialized output.
     */
    public LogicalProject( AlgInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalProject.
     */
    public static LogicalProject create( final AlgNode input, final List<? extends RexNode> projects, List<String> fieldNames ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgDataType rowType = RexUtil.createStructType( cluster.getTypeFactory(), projects, fieldNames, ValidatorUtil.F_SUGGESTER );
        return create( input, projects, rowType );
    }


    /**
     * Creates a LogicalProject, specifying row type rather than field names.
     */
    public static LogicalProject create( final AlgNode input, final List<? extends RexNode> projects, AlgDataType rowType ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSet()
                .replace( Convention.NONE )
                .replace( ModelTrait.RELATIONAL )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.project( mq, input, projects ) );
        return new LogicalProject( cluster, traitSet, input, projects, rowType );
    }


    public static LogicalProject identity( final AlgNode input ) {
        return create(
                input,
                IntStream.range( 0, input.getRowType().getFieldCount() )
                        .mapToObj( i ->
                                new RexInputRef( i, input.getRowType().getFieldList().get( i ).getType() )
                        )
                        .collect( Collectors.toList() ),
                input.getRowType()
        );
    }


    @Override
    public LogicalProject copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new LogicalProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}

