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

package org.polypheny.db.webui.models.catalog.requests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.runtime.Like;
import org.polypheny.db.webui.models.catalog.EntityModel;
import org.polypheny.db.webui.models.catalog.NamespaceModel;

public class SnapshotModel {

    public final long id;

    public final List<NamespaceModel> namespaces;

    public final List<EntityModel> entities;


    public SnapshotModel( long id, List<NamespaceModel> namespaces, List<EntityModel> entities ) {
        this.id = id;
        this.namespaces = ImmutableList.copyOf( namespaces );
        this.entities = ImmutableList.copyOf( entities );
    }


    public static SnapshotModel from( Snapshot snapshot ) {
        List<NamespaceModel> namespaces = snapshot.getNamespaces( null ).stream().map( NamespaceModel::from ).collect( Collectors.toList() );
        List<EntityModel> entities = new ArrayList<>();
        entities.addAll( snapshot.rel().getTables( (Pattern) null, null ).stream().map( EntityModel::from ).collect( Collectors.toList() ) );
        entities.addAll( snapshot.doc().getCollections( null, null ).stream().map( EntityModel::from ).collect( Collectors.toList() ) );
        entities.addAll( snapshot.graph().getGraphs( null ).stream().map( EntityModel::from ).collect( Collectors.toList() ) );

        return new SnapshotModel( snapshot.id(), namespaces, entities );
    }

}
