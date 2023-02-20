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

package org.polypheny.db.catalog.snapshot.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.logical.graph.CatalogGraph;
import org.polypheny.db.catalog.logical.graph.GraphCatalog;

@Value
public class LogicalGraphSnapshot implements LogicalSnapshot {

    ImmutableList<GraphCatalog> catalogs;
    public ImmutableList<CatalogGraph> graphs;

    public ImmutableMap<Long, CatalogGraph> graphIds;
    public ImmutableMap<String, CatalogGraph> graphNames;


    public LogicalGraphSnapshot( final List<GraphCatalog> catalogs ) {
        this.catalogs = ImmutableList.copyOf( catalogs.stream().map( GraphCatalog::copy ).collect( Collectors.toList() ) );

        this.graphs = ImmutableList.copyOf( buildGraphs() );
        this.graphIds = ImmutableMap.copyOf( buildGraphIds() );
        this.graphNames = ImmutableMap.copyOf( buildGraphNames() );

    }


    private List<CatalogGraph> buildGraphs() {
        return catalogs.stream().map( c -> new CatalogGraph( c.id, c.name ) ).collect( Collectors.toList() );
    }


    private Map<Long, CatalogGraph> buildGraphIds() {
        return graphs.stream().collect( Collectors.toMap( g -> g.id, g -> g ) );
    }


    private Map<String, CatalogGraph> buildGraphNames() {
        return graphs.stream().collect( Collectors.toMap( g -> g.name, g -> g ) );
    }


    @Override
    public NamespaceType getType() {
        return NamespaceType.GRAPH;
    }

}
