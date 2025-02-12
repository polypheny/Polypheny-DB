/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.polyalg.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph.SubstitutionGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.type.entity.PolyString;

public class EntityArg implements PolyAlgArg {

    @Getter
    private final Entity entity;

    private final String namespaceName;
    private String entityName; // for graphs, entityName is null

    // in the case of an AllocationEntity:
    private String partitionName;


    /**
     * Creates an EntityArg for an entity which is used in an AlgNode with the specified DataModel.
     */
    public EntityArg( Entity entity, Snapshot snapshot, DataModel model ) {
        this.namespaceName = getNamespaceName( entity, snapshot );
        this.entity = entity;

        if ( model == DataModel.GRAPH || entity.dataModel == DataModel.GRAPH ) {
            // origin or target data model is graph
            if ( entity instanceof SubstitutionGraph sub && !sub.names.isEmpty() ) {
                this.entityName = String.join( ".", sub.names.stream().map( PolyString::toString ).toList() );
            } else {
                this.entityName = null;
            }
        } else {
            this.entityName = entity.getName();
        }

        if ( entity instanceof AllocationEntity e ) {
            if ( e.dataModel != DataModel.GRAPH ) {
                this.entityName = snapshot.getLogicalEntity( e.logicalId ).orElseThrow().name;
            } else if ( !e.name.startsWith( AllocationEntity.PREFIX ) ) {
                this.entityName = e.name;
            }
            this.partitionName = snapshot.alloc().getPartition( e.partitionId ).orElseThrow().name;
        }
    }


    private String getNamespaceName( Entity entity, Snapshot snapshot ) {
        String nsName;
        try {
            nsName = entity.getNamespaceName();
        } catch ( UnsupportedOperationException e ) {
            Optional<LogicalNamespace> ns = snapshot.getNamespace( entity.namespaceId );
            nsName = ns.map( LogicalNamespace::getName ).orElse( null );
        }
        return nsName;
    }


    @Override
    public ParamType getType() {
        return ParamType.ENTITY;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        if ( entity instanceof PhysicalEntity e ) {
            return getAdapterName( e.adapterId ) + "." + e.id;
        }
        String name = getFullName();
        if ( entity instanceof AllocationEntity e ) {
            return name + "@" + getAdapterName( e.adapterId ) + "." + e.partitionId;
        }
        return name;
    }


    private String getAdapterName( Long adapterId ) {
        return AdapterManager.getInstance().getAdapter( adapterId ).orElseThrow().getUniqueName();
    }


    private String getFullName() {
        if ( entityName == null ) {
            return namespaceName;
        }
        return namespaceName + "." + entityName;
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();

        node.put( "fullName", getFullName() );

        if ( entity instanceof AllocationEntity e ) {
            node.put( "adapterName", getAdapterName( e.adapterId ) );
            node.put( "partitionId", String.valueOf( e.partitionId ) );
            if ( partitionName != null && !partitionName.isEmpty() ) {
                node.put( "partitionName", partitionName );
            }
        } else if ( entity instanceof PhysicalEntity e ) {
            node.put( "adapterName", getAdapterName( e.adapterId ) );
            node.put( "physicalId", e.id );
        }

        return node;
    }


}
