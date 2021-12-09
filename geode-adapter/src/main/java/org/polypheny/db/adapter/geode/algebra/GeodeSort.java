/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.geode.algebra;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link Sort} relational expression in Geode.
 */
public class GeodeSort extends Sort implements GeodeAlg {

    public static final String ASC = "ASC";
    public static final String DESC = "DESC";


    /**
     * Creates a GeodeSort.
     */
    GeodeSort( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgCollation collation, RexNode fetch ) {
        super( cluster, traitSet, input, collation, null, fetch );

        assert getConvention() == GeodeAlg.CONVENTION;
        assert getConvention() == input.getConvention();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        AlgOptCost cost = super.computeSelfCost( planner, mq );
        if ( fetch != null ) {
            return cost.multiplyBy( 0.05 );
        } else {
            return cost;
        }
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode input, AlgCollation newCollation, RexNode offset, RexNode fetch ) {
        return new GeodeSort( getCluster(), traitSet, input, collation, fetch );
    }


    @Override
    public void implement( GeodeImplementContext geodeImplementContext ) {
        geodeImplementContext.visitChild( getInput() );
        List<AlgFieldCollation> sortCollations = collation.getFieldCollations();
        if ( !sortCollations.isEmpty() ) {
            List<String> orderByFields = new ArrayList<>();
            for ( AlgFieldCollation fieldCollation : sortCollations ) {
                final String name = fieldName( fieldCollation.getFieldIndex() );
                orderByFields.add( name + " " + direction( fieldCollation.getDirection() ) );
            }
            geodeImplementContext.addOrderByFields( orderByFields );
        }

        if ( fetch != null ) {
            geodeImplementContext.setLimit( ((RexLiteral) fetch).getValueAs( Long.class ) );
        }
    }


    private String fieldName( int index ) {
        return getRowType().getFieldList().get( index ).getName();
    }


    private String direction( AlgFieldCollation.Direction algDirection ) {
        if ( algDirection == AlgFieldCollation.Direction.DESCENDING ) {
            return DESC;
        }
        return ASC;
    }

}

