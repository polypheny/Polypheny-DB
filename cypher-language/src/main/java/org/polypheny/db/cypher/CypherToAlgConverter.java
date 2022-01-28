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

package org.polypheny.db.cypher;

import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;

public class CypherToAlgConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final AlgOptCluster cluster;
    private final RexBuilder builder;
    private final AlgBuilder algBuilder;
    private final Statement statement;


    public CypherToAlgConverter( Statement statement, AlgBuilder builder, AlgOptCluster cluster ) {
        this.catalogReader = statement.getTransaction().getCatalogReader();
        this.statement = statement;
        this.cluster = cluster;
        this.builder = cluster.getRexBuilder();
        this.algBuilder = builder;
    }


    public AlgRoot convert( Node query, QueryParameters parameters ) {

        return null;
    }

}
