/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgDistributions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.SortExchange;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.arguments.CollationArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.IntArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Sub-class of {@link SortExchange} not targeted at any particular engine or calling convention.
 */
public class LogicalSortExchange extends SortExchange implements RelAlg {

    private LogicalSortExchange( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgDistribution distribution, AlgCollation collation ) {
        super( cluster, traitSet, input, distribution, collation );
    }


    /**
     * Creates a LogicalSortExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     * @param collation array of sort specifications
     */
    public static LogicalSortExchange create( AlgNode input, AlgDistribution distribution, AlgCollation collation ) {
        AlgCluster cluster = input.getCluster();
        collation = AlgCollationTraitDef.INSTANCE.canonize( collation );
        distribution = AlgDistributionTraitDef.INSTANCE.canonize( distribution );
        AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE ).replace( distribution ).replace( collation );
        return new LogicalSortExchange( cluster, traitSet, input, distribution, collation );
    }


    public static LogicalSortExchange create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        ListArg<CollationArg> collations = args.getListArg( "order", CollationArg.class );
        AlgDistribution.Type type = args.getEnumArg( "distributionType", AlgDistribution.Type.class ).getArg();
        List<Integer> numbers = args.getListArg( "numbers", IntArg.class ).map( IntArg::getArg );
        return create( children.get( 0 ),
                AlgDistributions.getDistribution( type, numbers ),
                AlgCollations.of( collations.map( CollationArg::getColl ) ) );
    }


    @Override
    public SortExchange copy( AlgTraitSet traitSet, AlgNode newInput, AlgDistribution newDistribution, AlgCollation newCollation ) {
        return new LogicalSortExchange( this.getCluster(), traitSet, newInput, newDistribution, newCollation );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        return args.put( "order", new ListArg<>( collation.getFieldCollations(), CollationArg::new ) )
                .put( "distributionType", new EnumArg<>( distribution.getType(), ParamType.DISTRIBUTION_TYPE_ENUM ) )
                .put( "numbers", new ListArg<>( distribution.getKeys(), IntArg::new ) );
    }

}
