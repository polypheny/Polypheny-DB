/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;

import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of {@link Sort} relational expression in Geode.
 */
public class GeodeSort extends Sort implements GeodeRel {

    public static final String ASC = "ASC";
    public static final String DESC = "DESC";


    /**
     * Creates a GeodeSort.
     */
    GeodeSort( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode fetch ) {
        super( cluster, traitSet, input, collation, null, fetch );

        assert getConvention() == GeodeRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        RelOptCost cost = super.computeSelfCost( planner, mq );
        if ( fetch != null ) {
            return cost.multiplyBy( 0.05 );
        } else {
            return cost;
        }
    }


    @Override
    public Sort copy( RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch ) {
        return new GeodeSort( getCluster(), traitSet, input, collation, fetch );
    }


    @Override
    public void implement( GeodeImplementContext geodeImplementContext ) {
        geodeImplementContext.visitChild( getInput() );
        List<RelFieldCollation> sortCollations = collation.getFieldCollations();
        if ( !sortCollations.isEmpty() ) {
            List<String> orderByFields = new ArrayList<>();
            for ( RelFieldCollation fieldCollation : sortCollations ) {
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


    private String direction( RelFieldCollation.Direction relDirection ) {
        if ( relDirection == RelFieldCollation.Direction.DESCENDING ) {
            return DESC;
        }
        return ASC;
    }
}

