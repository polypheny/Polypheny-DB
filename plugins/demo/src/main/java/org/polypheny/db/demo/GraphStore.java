/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo;

import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;

public class GraphStore extends DemoStore {

    private final static String[] files = new String[]{"/musicbrainz/artist.json"};
    private final TransactionManager transactionManager;

    public GraphStore( TransactionManager transactionManager, boolean local ) {
        super("demoneo4j", "cypher", DataModel.GRAPH, "neo4j" );
        this.transactionManager = transactionManager;
    }


    @Override
    public void setupNamespace( Statement statement ) {

    }


    @Override
    public void loadData() {

    }

}
