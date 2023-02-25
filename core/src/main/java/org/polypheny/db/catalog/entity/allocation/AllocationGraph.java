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

package org.polypheny.db.catalog.entity.allocation;

import java.io.Serializable;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;

public class AllocationGraph extends CatalogEntity implements Allocation {


    public final LogicalGraph logical;
    public final long id;


    public AllocationGraph( long id, LogicalGraph graph ) {
        super( id, graph.name, graph.entityType, graph.namespaceType );
        this.id = id;
        this.logical = graph;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }



}
