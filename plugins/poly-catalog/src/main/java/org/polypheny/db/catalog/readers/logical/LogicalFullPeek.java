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

package org.polypheny.db.catalog.readers.logical;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.logical.document.DocumentCatalog;
import org.polypheny.db.catalog.logical.graph.GraphCatalog;
import org.polypheny.db.catalog.logical.relational.RelationalCatalog;

public class LogicalFullPeek implements LogicalPeek {

    private final ImmutableMap<Long, NCatalog> catalogs;
    private final LogicalRelationalPeek relationalPeek;
    private final LogicalGraphPeek graphPeek;
    private final LogicalDocumentPeek documentPeek;
    private final ImmutableMap<Long, LogicalPeek> ids;
    private final ImmutableMap<String, LogicalPeek> names;


    public LogicalFullPeek( Map<Long, NCatalog> catalogs ) {
        this.catalogs = ImmutableMap.copyOf( catalogs );

        List<RelationalCatalog> relational = catalogs.values().stream().filter( c -> c.getType() == NamespaceType.RELATIONAL ).map( NCatalog::asRelational ).collect( Collectors.toList() );
        List<GraphCatalog> graph = catalogs.values().stream().filter( c -> c.getType() == NamespaceType.GRAPH ).map( NCatalog::asGraph ).collect( Collectors.toList() );
        List<DocumentCatalog> document = catalogs.values().stream().filter( c -> c.getType() == NamespaceType.DOCUMENT ).map( NCatalog::asDocument ).collect( Collectors.toList() );

        this.relationalPeek = new LogicalRelationalPeek( relational );
        this.graphPeek = new LogicalGraphPeek( graph );
        this.documentPeek = new LogicalDocumentPeek( document );

        this.ids = buildIds();
        this.names = buildNames();
    }


    private ImmutableMap<Long, LogicalPeek> buildIds() {
        Map<Long, LogicalPeek> ids = new HashMap<>();
        this.relationalPeek.schemaIds.keySet().forEach( id -> ids.put( id, relationalPeek ) );
        this.graphPeek.graphIds.keySet().forEach( id -> ids.put( id, graphPeek ) );
        this.documentPeek.databaseIds.keySet().forEach( id -> ids.put( id, documentPeek ) );

        return ImmutableMap.copyOf( ids );
    }


    private ImmutableMap<String, LogicalPeek> buildNames() {
        Map<String, LogicalPeek> names = new HashMap<>();
        this.relationalPeek.schemaNames.keySet().forEach( name -> names.put( name, relationalPeek ) );
        this.graphPeek.graphNames.keySet().forEach( name -> names.put( name, graphPeek ) );
        this.documentPeek.databaseNames.keySet().forEach( name -> names.put( name, documentPeek ) );

        return ImmutableMap.copyOf( names );
    }


    @Override
    public NamespaceType getType() {
        return null;
    }

}
