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

import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.LiteralStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.algebra.core.lpg.LpgCall;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import java.util.ArrayList;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.call_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.create_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.delete_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.edge_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.path_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.set_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.string_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.yield_;

public class NeoLpgCall extends LpgCall implements NeoGraphAlg {


    public NeoLpgCall( AlgCluster cluster, AlgTraitSet traits, ArrayList<String> namespace, String procedureName, ArrayList<PolyValue> arguments, Adapter procedureProvider, boolean yieldAll, ArrayList<String> yieldItems ) {
        super( cluster, traits, namespace, procedureName, arguments, procedureProvider, yieldAll, yieldItems );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        String neoProcedureName = String.join( ".", getNamespace() ) + "." + getProcedureName();

        ArrayList<NeoStatement> literalArgs = new ArrayList<>();
        for ( PolyValue argument : getArguments() ) {
            literalArgs.add( literal_( argument ) );
        }

        boolean yieldAll = isYieldAll();
        ArrayList<String> yieldItems = new ArrayList<>();

        if ( getYieldItems() != null ) {
            for ( String yieldItem : getYieldItems() ) {
                yieldItems.add( yieldItem );
            }
        } else {
            yieldAll = true;
        }

        implementor.add( call_( neoProcedureName, literalArgs.size() == 0, list_( literalArgs, "(", ")" ), yieldAll, yieldItems ) );
    }

    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return planner.getCostFactory().makeTinyCost();
    }

}
