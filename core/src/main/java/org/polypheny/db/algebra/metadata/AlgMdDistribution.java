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

package org.polypheny.db.algebra.metadata;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.mapping.Mappings;


/**
 * RelMdCollation supplies a default implementation of {@link AlgMetadataQuery#distribution} for the standard logical algebra.
 */
public class AlgMdDistribution implements MetadataHandler<BuiltInMetadata.Distribution> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdDistribution(), BuiltInMethod.DISTRIBUTION.method );


    private AlgMdDistribution() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Distribution> getDef() {
        return BuiltInMetadata.Distribution.DEF;
    }


    /**
     * Fallback method to deduce distribution for any relational expression not handled by a more specific method.
     *
     * @param alg Relational expression
     * @return Relational expression's distribution
     */
    public AlgDistribution distribution( AlgNode alg, AlgMetadataQuery mq ) {
        return AlgDistributions.SINGLETON;
    }


    public AlgDistribution distribution( SingleAlg alg, AlgMetadataQuery mq ) {
        return mq.distribution( alg.getInput() );
    }


    public AlgDistribution distribution( BiAlg alg, AlgMetadataQuery mq ) {
        return mq.distribution( alg.getLeft() );
    }


    public AlgDistribution distribution( SetOp alg, AlgMetadataQuery mq ) {
        return mq.distribution( alg.getInputs().get( 0 ) );
    }


    public AlgDistribution distribution( RelScan<?> scan, AlgMetadataQuery mq ) {
        return table( scan.getEntity() );
    }


    public AlgDistribution distribution( Project project, AlgMetadataQuery mq ) {
        return project( mq, project.getInput(), project.getProjects() );
    }


    public AlgDistribution distribution( Values values, AlgMetadataQuery mq ) {
        return values( values.getTupleType(), values.getTuples() );
    }


    public AlgDistribution distribution( Exchange exchange, AlgMetadataQuery mq ) {
        return exchange( exchange.distribution );
    }


    public AlgDistribution distribution( HepAlgVertex alg, AlgMetadataQuery mq ) {
        return mq.distribution( alg.getCurrentAlg() );
    }


    /**
     * Helper method to determine a {@link RelScan}'s distribution.
     */
    public static AlgDistribution table( Entity table ) {
        return table.getDistribution();
    }


    /**
     * Helper method to determine a {@link Sort}'s distribution.
     */
    public static AlgDistribution sort( AlgMetadataQuery mq, AlgNode input ) {
        return mq.distribution( input );
    }


    /**
     * Helper method to determine a {@link Filter}'s distribution.
     */
    public static AlgDistribution filter( AlgMetadataQuery mq, AlgNode input ) {
        return mq.distribution( input );
    }


    /**
     * Helper method to determine a limit's distribution.
     */
    public static AlgDistribution limit( AlgMetadataQuery mq, AlgNode input ) {
        return mq.distribution( input );
    }


    /**
     * Helper method to determine a {@link org.polypheny.db.algebra.core.Calc}'s distribution.
     */
    public static AlgDistribution calc( AlgMetadataQuery mq, AlgNode input, RexProgram program ) {
        throw new AssertionError(); // TODO:
    }


    /**
     * Helper method to determine a {@link Project}'s collation.
     */
    public static AlgDistribution project( AlgMetadataQuery mq, AlgNode input, List<? extends RexNode> projects ) {
        final AlgDistribution inputDistribution = mq.distribution( input );
        final Mappings.TargetMapping mapping = Project.getPartialMapping( input.getTupleType().getFieldCount(), projects );
        return inputDistribution.apply( mapping );
    }


    /**
     * Helper method to determine a {@link Values}'s distribution.
     */
    public static AlgDistribution values( AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        return AlgDistributions.BROADCAST_DISTRIBUTED;
    }


    /**
     * Helper method to determine an {@link Exchange}'s or {@link org.polypheny.db.algebra.core.SortExchange}'s distribution.
     */
    public static AlgDistribution exchange( AlgDistribution distribution ) {
        return distribution;
    }

}

