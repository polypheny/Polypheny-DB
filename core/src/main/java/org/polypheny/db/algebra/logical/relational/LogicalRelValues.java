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


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Sub-class of {@link org.polypheny.db.algebra.core.Values} not targeted at any particular engine or calling convention.
 */
public class LogicalRelValues extends Values implements RelAlg {

    /**
     * Creates a LogicalValues.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param rowType Row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer list contains tuples; each inner list is one tuple; all tuples must be of same length, conforming to rowType
     */
    public LogicalRelValues( AlgCluster cluster, AlgTraitSet traitSet, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        super( cluster, rowType, tuples, traitSet.replace( ModelTrait.RELATIONAL ) );
    }


    /**
     * Creates a LogicalValues.
     */
    public static LogicalRelValues create( AlgCluster cluster, final AlgDataType rowType, final ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.values( mq, rowType, tuples ) );
        return new LogicalRelValues( cluster, traitSet, rowType, tuples );
    }


    public static LogicalRelValues create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        List<String> names = args.getListArg( "names", StringArg.class ).map( StringArg::getArg );
        List<List<RexLiteral>> tuples = PolyAlgUtils.getNestedListArgAsList(
                args.getListArg( "tuples", ListArg.class ),
                r -> (RexLiteral) ((RexArg) r).getNode() );

        AlgDataType rowType = RexUtil.createStructType( cluster.getTypeFactory(), tuples.get( 0 ), names, ValidatorUtil.F_SUGGESTER );
        return create( cluster, rowType, PolyAlgUtils.toImmutableNestedList( tuples ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return new LogicalRelValues( getCluster(), traitSet, rowType, tuples );
    }


    /**
     * Creates a LogicalValues that outputs no rows of a given row type.
     */
    public static LogicalRelValues createEmpty( AlgCluster cluster, AlgDataType rowType ) {
        return create( cluster, rowType, ImmutableList.of() );
    }


    /**
     * Creates a LogicalValues that outputs one row and one column.
     */
    public static LogicalRelValues createOneRow( AlgCluster cluster ) {
        final AlgDataType rowType =
                cluster.getTypeFactory()
                        .builder()
                        .add( "ZERO", null, PolyType.INTEGER )
                        .nullable( false )
                        .build();
        final ImmutableList<ImmutableList<RexLiteral>> tuples =
                ImmutableList.of(
                        ImmutableList.of(
                                cluster.getRexBuilder().makeExactLiteral(
                                        BigDecimal.ZERO,
                                        rowType.getFields().get( 0 ).getType() ) ) );
        return create( cluster, rowType, tuples );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        args.put( "names", new ListArg<>( rowType.getFieldNames(), StringArg::new ) );

        List<ListArg<RexArg>> tuplesArg = new ArrayList<>();
        for ( ImmutableList<RexLiteral> tuple : getTuples() ) {
            tuplesArg.add( new ListArg<>( tuple, RexArg::new ) );
        }
        args.put( "tuples", new ListArg<>( tuplesArg ) );

        return args;
    }

}

