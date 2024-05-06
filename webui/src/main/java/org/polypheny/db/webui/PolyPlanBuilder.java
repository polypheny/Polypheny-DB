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

package org.polypheny.db.webui;

import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgParser;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgToAlgConverter;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;

public class PolyPlanBuilder {

    private PolyPlanBuilder() {
        // This is a utility class
    }


    /**
     * Creates a AlgNode tree from the given PolyAlg representation
     *
     * @param polyAlg string representing the AlgNode tree serialized as PolyAlg
     * @param statement transaction statement
     * @return AlgRoot with {@code AlgRoot.alg} being the top node of tree
     * @throws NodeParseException if the parser is not able to construct the intermediary PolyAlgNode tree
     * @throws RuntimeException if polyAlg cannot be parsed into a valid AlgNode tree
     */
    public static AlgRoot buildFromPolyAlg( String polyAlg, Statement statement ) throws NodeParseException {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        AlgCluster cluster = AlgCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, snapshot );
        PolyAlgToAlgConverter converter = new PolyAlgToAlgConverter( snapshot, cluster );

        PolyAlgParser parser = PolyAlgParser.create( polyAlg );
        PolyAlgNode node = (PolyAlgNode) parser.parseQuery();
        return converter.convert( node );
    }

}
