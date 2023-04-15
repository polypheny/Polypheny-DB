/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.polyfier.core.construct;

import lombok.Getter;
import org.polypheny.db.polyfier.core.construct.graphs.ColumnGraph;
import org.polypheny.db.polyfier.core.construct.graphs.DecisionGraph;
import org.polypheny.db.polyfier.core.construct.graphs.QueryGraph;
import org.polypheny.db.polyfier.core.construct.model.Decision;
import org.polypheny.db.transaction.Statement;

@Getter
public class ConstructionGraph {
    private final Statement statement;
    private final QueryGraph queryGraph;
    private final ColumnGraph columnGraph;
    private DecisionGraph decisionGraph;

    public ConstructionGraph( Statement statement ) {
        this.columnGraph = new ColumnGraph();
        this.queryGraph = new QueryGraph();

        this.decisionGraph = new DecisionGraph();
        this.statement = statement;
    }

    public void clearDecisionGraph() {
        this.decisionGraph = new DecisionGraph( Decision.root() );
    }

}
