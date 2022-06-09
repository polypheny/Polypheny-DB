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
 */

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.limit_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.orderBy_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.skip_;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.graph.GraphSort;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;

public class NeoGraphSort extends GraphSort implements NeoGraphAlg {

    public NeoGraphSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child, collation, offset, fetch );
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
        return new NeoGraphSort( input.getCluster(), traitSet, input, collation, offset, fetch );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        implementor.addReturnIfNecessary();
        implementor.setSorted( true );

        List<String> lastNames = implementor.getLast().getRowType().getFieldNames();

        List<String> groups = new ArrayList<>();
        for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
            groups.add( lastNames.get( fieldCollation.getFieldIndex() ) + (fieldCollation.direction == Direction.ASCENDING ? "" : " DESC ") );
        }

        if ( !groups.isEmpty() ) {
            implementor.add( orderBy_( list_( groups.stream().map( NeoStatements::literal_ ).collect( Collectors.toList() ) ) ) );
        }

        if ( offset != null ) {
            implementor.add( skip_( ((RexLiteral) offset).getValueAs( Integer.class ) ) );
        }

        if ( fetch != null ) {
            implementor.add( limit_( ((RexLiteral) fetch).getValueAs( Integer.class ) ) );
        }

    }

}
