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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo;

import lombok.Getter;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs.ConstructionGraph;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs.DecisionGraph;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs.RCN;
import org.polypheny.db.transaction.Statement;

@Getter
public class PolyGraph {
    private DecisionGraph decisionGraph;
    private RCN rcn;
    private ConstructionGraph constructionGraph;
    private final Statement statement;

    public PolyGraph( Statement statement ) {
        this.rcn = new RCN();
        this.constructionGraph = new ConstructionGraph();
        this.decisionGraph = new DecisionGraph();
        this.statement = statement;
    }

    public void clearDecisionGraph() {
        this.decisionGraph = new DecisionGraph();
    }

    public void clearConstructionGraph() {
        this.constructionGraph = new ConstructionGraph();
    }

    public void clearRCN() {
        this.rcn = new RCN();
    }

}
