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

package org.polypheny.db.webui.models.catalog;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.webui.models.AdapterTemplateModel;
import org.polypheny.db.webui.models.catalog.schema.AllocationColumnModel;
import org.polypheny.db.webui.models.catalog.schema.AllocationEntityModel;
import org.polypheny.db.webui.models.catalog.schema.AllocationPartitionModel;
import org.polypheny.db.webui.models.catalog.schema.AllocationPlacementModel;
import org.polypheny.db.webui.models.catalog.schema.CollectionModel;
import org.polypheny.db.webui.models.catalog.schema.ColumnModel;
import org.polypheny.db.webui.models.catalog.schema.ConstraintModel;
import org.polypheny.db.webui.models.catalog.schema.EntityModel;
import org.polypheny.db.webui.models.catalog.schema.FieldModel;
import org.polypheny.db.webui.models.catalog.schema.GraphModel;
import org.polypheny.db.webui.models.catalog.schema.KeyModel;
import org.polypheny.db.webui.models.catalog.schema.NamespaceModel;
import org.polypheny.db.webui.models.catalog.schema.TableModel;

@Value
public class SnapshotModel {

    public long id;

    public List<NamespaceModel> namespaces;

    public List<EntityModel> entities;

    public List<FieldModel> fields;
    public List<KeyModel> keys;
    public List<ConstraintModel> constraints;
    public List<AllocationEntityModel> allocations;
    public List<AllocationPlacementModel> placements;
    public List<AllocationPartitionModel> partitions;
    public List<AllocationColumnModel> allocColumns;
    public List<AdapterModel> adapters;
    public List<AdapterTemplateModel> adapterTemplates;


    public SnapshotModel(
            long id,
            List<NamespaceModel> namespaces,
            List<EntityModel> entities,
            List<FieldModel> fields,
            List<KeyModel> keys,
            List<ConstraintModel> constraints,
            List<AllocationEntityModel> allocations,
            List<AllocationPlacementModel> placements,
            List<AllocationPartitionModel> partitions,
            List<AllocationColumnModel> allocColumns,
            List<AdapterModel> adapters,
            List<AdapterTemplateModel> adapterTemplates ) {
        this.id = id;
        this.namespaces = ImmutableList.copyOf( namespaces );
        this.entities = ImmutableList.copyOf( entities );
        this.fields = ImmutableList.copyOf( fields );
        this.keys = ImmutableList.copyOf( keys );
        this.constraints = ImmutableList.copyOf( constraints );
        this.allocations = ImmutableList.copyOf( allocations );
        this.placements = ImmutableList.copyOf( placements );
        this.partitions = ImmutableList.copyOf( partitions );
        this.allocColumns = ImmutableList.copyOf( allocColumns );
        this.adapters = ImmutableList.copyOf( adapters );
        this.adapterTemplates = ImmutableList.copyOf( adapterTemplates );
    }


    public static SnapshotModel from( Snapshot snapshot ) {
        List<NamespaceModel> namespaces = snapshot.getNamespaces( null ).stream().map( NamespaceModel::from ).collect( Collectors.toList() );
        List<EntityModel> entities = new ArrayList<>();
        entities.addAll( snapshot.rel().getTables( (Pattern) null, null ).stream().map( TableModel::from ).collect( Collectors.toList() ) );
        entities.addAll( snapshot.doc().getCollections( null, null ).stream().map( CollectionModel::from ).collect( Collectors.toList() ) );
        entities.addAll( snapshot.graph().getGraphs( null ).stream().map( GraphModel::from ).collect( Collectors.toList() ) );

        List<FieldModel> fields = snapshot.rel().getColumns( null, null ).stream().map( ColumnModel::from ).collect( Collectors.toList() );

        List<KeyModel> keys = new ArrayList<>();
        keys.addAll( snapshot.rel().getKeys().stream().map( KeyModel::from ).collect( Collectors.toList() ) );
        keys.addAll( snapshot.rel().getPrimaryKeys().stream().map( KeyModel::from ).collect( Collectors.toList() ) );

        List<ConstraintModel> constraints = snapshot.rel().getConstraints().stream().map( ConstraintModel::from ).collect( Collectors.toList() );

        List<AllocationEntityModel> allocations = snapshot.alloc().getAllocations().stream().map( AllocationEntityModel::from ).collect( Collectors.toList() );

        List<AllocationPlacementModel> placements = snapshot.alloc().getPlacements().stream().map( AllocationPlacementModel::from ).collect( Collectors.toList() );

        List<AllocationPartitionModel> partitions = snapshot.alloc().getPartitions().stream().map( AllocationPartitionModel::from ).collect( Collectors.toList() );

        List<AllocationColumnModel> allocationColumns = snapshot.alloc().getColumns().stream().map( AllocationColumnModel::from ).collect( Collectors.toList() );

        List<AdapterModel> adapters = snapshot.getAdapters().stream().map( AdapterModel::from ).collect( Collectors.toList() );

        List<AdapterTemplateModel> adapterTemplates = snapshot.getAdapterTemplates().stream().map( AdapterTemplateModel::from ).collect( Collectors.toList() );

        return new SnapshotModel( snapshot.id(), namespaces, entities, fields, keys, constraints, allocations, placements, partitions, allocationColumns, adapters, adapterTemplates );
    }

}
