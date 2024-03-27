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

package org.polypheny.db.adapter.mongodb.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.util.RexToMongoTranslator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.Pair;

public class MongoDocumentProject extends DocumentProject implements MongoAlg {

    /**
     * Creates a {@link DocumentProject}.
     * {@link ModelTrait#DOCUMENT} native node of a project.
     *
     * @param cluster
     * @param traits
     * @param input
     * @param includes
     * @param excludes
     */
    protected MongoDocumentProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, @NotNull Map<String, ? extends RexNode> includes, @NotNull List<String> excludes ) {
        super( cluster, traits, input, includes, excludes );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        List<Pair<String, String>> projects = new ArrayList<>();

        final RexToMongoTranslator translator = new RexToMongoTranslator( List.of(), implementor, DataModel.DOCUMENT );

        // is it something which interacts with root?
        if ( excludes.isEmpty() && includes.size() == 1 && includes.containsKey( null ) ) {
            adjustRoot( implementor, translator );
            return;
        }

        includes.forEach( ( n, p ) -> projects.add( Pair.of( n, p.accept( translator ) ) ) );
        excludes.forEach( n -> projects.add( Pair.of( n, "0" ) ) );

        String merged = projects.stream().map( p -> "\"" + p.left + "\":" + p.right ).collect( Collectors.joining( "," ) );

        implementor.add( merged, "{$project: {" + merged + "}}" );

    }


    private void adjustRoot( Implementor implementor, RexToMongoTranslator translator ) {
        String merged = includes.get( null ).accept( translator );
        implementor.add( null, merged );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new MongoDocumentProject( getCluster(), traitSet, sole( inputs ), includes, excludes );
    }

}
