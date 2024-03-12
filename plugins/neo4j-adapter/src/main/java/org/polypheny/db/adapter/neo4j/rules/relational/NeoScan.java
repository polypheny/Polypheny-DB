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

package org.polypheny.db.adapter.neo4j.rules.relational;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.match_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.neo4j.NeoEntity;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyString;

public class NeoScan extends RelScan<NeoEntity> implements NeoRelAlg {


    public NeoScan( AlgCluster cluster, AlgTraitSet traitSet, NeoEntity neoEntity ) {
        super( cluster, traitSet, neoEntity );
    }


    @Override
    public void implement( NeoRelationalImplementor implementor ) {
        if ( implementor.getEntity() != null && !Objects.equals( entity.id, implementor.getEntity().id ) ) {
            handleInsertFromOther( implementor );
            return;
        }

        implementor.setEntity( entity );

        implementor.add( match_( node_( PolyString.of( entity.name ), labels_( PolyString.of( entity.name ) ) ) ) );

        if ( !implementor.isDml() || !implementor.isPrepared() ) {
            List<NeoStatement> mapping = entity
                    .getTupleType()
                    .getFields()
                    .stream().map( f -> (NeoStatement) as_( literal_( PolyString.of( entity.name + "." + f.getPhysicalName() ) ), literal_( PolyString.of( f.getName() ) ) ) )
                    .toList();

            if ( implementor.isDml() ) {
                mapping = new ArrayList<>( mapping );
                mapping.add( literal_( PolyString.of( entity.name ) ) );
            }

            implementor.add( with_( list_( mapping ) ) );
        }
    }


    private void handleInsertFromOther( NeoRelationalImplementor implementor ) {
        implementor.selectFromTable = entity;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoScan( getCluster(), traitSet, entity );
    }

}
