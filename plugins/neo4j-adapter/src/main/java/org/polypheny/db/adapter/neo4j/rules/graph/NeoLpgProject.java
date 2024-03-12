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
 */

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.return_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.Translator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyString;

public class NeoLpgProject extends LpgProject implements NeoGraphAlg {

    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgProject}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link ModelTrait#GRAPH}
     * @param input Input algebraic expression
     */
    public NeoLpgProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<PolyString> names, List<? extends RexNode> projects ) {
        super( cluster, traits, input, projects, names );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );

        if ( !implementor.isDml() ) {

            int i = 0;
            List<NeoStatement> statements = new ArrayList<>();
            for ( RexNode project : getProjects() ) {
                Translator translator = new Translator( getTupleType(), implementor.getLast().getTupleType(), new HashMap<>(), null, implementor.getGraph().mappingLabel, false );
                String name = project.accept( translator );
                if ( names.get( i ) != null && !name.equals( names.get( i ).value ) && !names.get( i ).value.contains( "." ) && !name.equals( "*" ) ) {
                    statements.add( as_( literal_( PolyString.of( project.accept( translator ) ) ), literal_( names.get( i ) ) ) );
                } else {
                    statements.add( literal_( PolyString.of( project.accept( translator ) ) ) );
                }

                i++;
            }

            if ( implementor.isSorted() ) {
                implementor.replaceReturn( return_( list_( statements ) ) );
                return;
            }

            implementor.add( with_( list_( statements ) ) );
        }

    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoLpgProject( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), names, projects );
    }

}
