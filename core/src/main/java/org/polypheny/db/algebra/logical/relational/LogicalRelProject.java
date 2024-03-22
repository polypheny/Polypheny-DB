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

package org.polypheny.db.algebra.logical.relational;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Subclass of {@link Project} not targeted at any particular engine or calling convention.
 */
public final class LogicalRelProject extends Project implements RelAlg {

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
    public LogicalRelProject( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), input, projects, rowType );
        assert traitSet.containsIfApplicable( Convention.NONE );
    }


    /**
     * Creates a LogicalProject.
     */
    public static LogicalRelProject create( final AlgNode input, final List<? extends RexNode> projects, List<String> fieldNames ) {
        final AlgCluster cluster = input.getCluster();
        final AlgDataType rowType = RexUtil.createStructType( cluster.getTypeFactory(), projects, fieldNames, ValidatorUtil.F_SUGGESTER );
        return create( input, projects, rowType );
    }


    /**
     * Creates a LogicalProject, specifying row type rather than field names.
     */
    public static LogicalRelProject create( final AlgNode input, final List<? extends RexNode> projects, AlgDataType rowType ) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSet()
                .replace( Convention.NONE )
                //.replace( ModelTrait.RELATIONAL )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.project( mq, input, projects ) );
        return new LogicalRelProject( cluster, traitSet, input, projects, rowType );
    }


    public static LogicalRelProject identity( final AlgNode input ) {
        return create(
                input,
                IntStream.range( 0, input.getTupleType().getFieldCount() )
                        .mapToObj( i ->
                                new RexIndexRef( i, input.getTupleType().getFields().get( i ).getType() )
                        )
                        .collect( Collectors.toList() ),
                input.getTupleType()
        );
    }


    @Override
    public LogicalRelProject copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new LogicalRelProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );
        PolyAlgArg projectsArg = new ListArg<>( exps, e -> new RexArg( e, this ), rowType.getFieldNames(), args.getDecl().canUnpackValues() );

        args.put( 0, projectsArg );
        return args;
    }

}

