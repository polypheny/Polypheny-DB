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

package org.polypheny.db.rel.metadata;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.hep.HepRelVertex;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelDistribution;
import org.polypheny.db.rel.RelDistributions;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.core.Exchange;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.SetOp;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.mapping.Mappings;


/**
 * RelMdCollation supplies a default implementation of {@link RelMetadataQuery#distribution} for the standard logical algebra.
 */
public class RelMdDistribution implements MetadataHandler<BuiltInMetadata.Distribution> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.DISTRIBUTION.method, new RelMdDistribution() );


    private RelMdDistribution() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Distribution> getDef() {
        return BuiltInMetadata.Distribution.DEF;
    }


    /**
     * Fallback method to deduce distribution for any relational expression not handled by a more specific method.
     *
     * @param rel Relational expression
     * @return Relational expression's distribution
     */
    public RelDistribution distribution( RelNode rel, RelMetadataQuery mq ) {
        return RelDistributions.SINGLETON;
    }


    public RelDistribution distribution( SingleRel rel, RelMetadataQuery mq ) {
        return mq.distribution( rel.getInput() );
    }


    public RelDistribution distribution( BiRel rel, RelMetadataQuery mq ) {
        return mq.distribution( rel.getLeft() );
    }


    public RelDistribution distribution( SetOp rel, RelMetadataQuery mq ) {
        return mq.distribution( rel.getInputs().get( 0 ) );
    }


    public RelDistribution distribution( TableScan scan, RelMetadataQuery mq ) {
        return table( scan.getTable() );
    }


    public RelDistribution distribution( Project project, RelMetadataQuery mq ) {
        return project( mq, project.getInput(), project.getProjects() );
    }


    public RelDistribution distribution( Values values, RelMetadataQuery mq ) {
        return values( values.getRowType(), values.getTuples() );
    }


    public RelDistribution distribution( Exchange exchange, RelMetadataQuery mq ) {
        return exchange( exchange.distribution );
    }


    public RelDistribution distribution( HepRelVertex rel, RelMetadataQuery mq ) {
        return mq.distribution( rel.getCurrentRel() );
    }


    /**
     * Helper method to determine a {@link TableScan}'s distribution.
     */
    public static RelDistribution table( RelOptTable table ) {
        return table.getDistribution();
    }


    /**
     * Helper method to determine a {@link Sort}'s distribution.
     */
    public static RelDistribution sort( RelMetadataQuery mq, RelNode input ) {
        return mq.distribution( input );
    }


    /**
     * Helper method to determine a {@link Filter}'s distribution.
     */
    public static RelDistribution filter( RelMetadataQuery mq, RelNode input ) {
        return mq.distribution( input );
    }


    /**
     * Helper method to determine a limit's distribution.
     */
    public static RelDistribution limit( RelMetadataQuery mq, RelNode input ) {
        return mq.distribution( input );
    }


    /**
     * Helper method to determine a {@link org.polypheny.db.rel.core.Calc}'s distribution.
     */
    public static RelDistribution calc( RelMetadataQuery mq, RelNode input, RexProgram program ) {
        throw new AssertionError(); // TODO:
    }


    /**
     * Helper method to determine a {@link Project}'s collation.
     */
    public static RelDistribution project( RelMetadataQuery mq, RelNode input, List<? extends RexNode> projects ) {
        final RelDistribution inputDistribution = mq.distribution( input );
        final Mappings.TargetMapping mapping = Project.getPartialMapping( input.getRowType().getFieldCount(), projects );
        return inputDistribution.apply( mapping );
    }


    /**
     * Helper method to determine a {@link Values}'s distribution.
     */
    public static RelDistribution values( RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        return RelDistributions.BROADCAST_DISTRIBUTED;
    }


    /**
     * Helper method to determine an {@link Exchange}'s or {@link org.polypheny.db.rel.core.SortExchange}'s distribution.
     */
    public static RelDistribution exchange( RelDistribution distribution ) {
        return distribution;
    }
}

