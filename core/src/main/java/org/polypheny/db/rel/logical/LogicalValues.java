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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;


/**
 * Sub-class of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Values} not targeted at any particular engine or calling convention.
 */
public class LogicalValues extends Values {

    /**
     * Creates a LogicalValues.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param rowType Row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer list contains tuples; each inner list is one tuple; all tuples must be of same length, conforming to rowType
     */
    public LogicalValues( RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        super( cluster, rowType, tuples, traitSet );
    }


    /**
     * Creates a LogicalValues by parsing serialized output.
     */
    public LogicalValues( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalValues.
     */
    public static LogicalValues create( RelOptCluster cluster, final RelDataType rowType, final ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.values( mq, rowType, tuples ) );
        return new LogicalValues( cluster, traitSet, rowType, tuples );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return new LogicalValues( getCluster(), traitSet, rowType, tuples );
    }


    /**
     * Creates a LogicalValues that outputs no rows of a given row type.
     */
    public static LogicalValues createEmpty( RelOptCluster cluster, RelDataType rowType ) {
        return create( cluster, rowType, ImmutableList.of() );
    }


    /**
     * Creates a LogicalValues that outputs one row and one column.
     */
    public static LogicalValues createOneRow( RelOptCluster cluster ) {
        final RelDataType rowType =
                cluster.getTypeFactory()
                        .builder()
                        .add( "ZERO", null, SqlTypeName.INTEGER )
                        .nullable( false )
                        .build();
        final ImmutableList<ImmutableList<RexLiteral>> tuples =
                ImmutableList.of(
                        ImmutableList.of(
                                cluster.getRexBuilder().makeExactLiteral(
                                        BigDecimal.ZERO,
                                        rowType.getFieldList().get( 0 ).getType() ) ) );
        return create( cluster, rowType, tuples );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}

