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

package org.polypheny.db.adapter.neo4j.rules.relational;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.match_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.neo4j.NeoEntity;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;

public class NeoScan extends Scan implements NeoRelAlg {


    private final NeoEntity neoEntity;


    public NeoScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, NeoEntity neoEntity ) {
        super( cluster, traitSet, table );
        this.neoEntity = neoEntity;
    }


    @Override
    public void implement( NeoRelationalImplementor implementor ) {
        implementor.setTable( table );

        implementor.add( match_( node_( neoEntity.physicalEntityName, labels_( neoEntity.physicalEntityName ) ) ) );

        if ( !implementor.isDml() ) {
            List<NeoStatement> mapping = table
                    .getRowType()
                    .getFieldList()
                    .stream().map( f -> as_( literal_( neoEntity.physicalEntityName + "." + f.getPhysicalName() ), literal_( f.getName() ) ) )
                    .collect( Collectors.toList() );

            implementor.add( with_( list_( mapping ) ) );
        }
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoScan( getCluster(), traitSet, getTable(), neoEntity );
    }

}
