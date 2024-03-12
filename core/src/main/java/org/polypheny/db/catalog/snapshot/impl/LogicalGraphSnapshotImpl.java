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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;

@Slf4j
@EqualsAndHashCode
public class LogicalGraphSnapshotImpl implements LogicalGraphSnapshot {

    private final ImmutableMap<Long, LogicalGraph> graphs;

    private final ImmutableMap<String, LogicalGraph> graphNames;


    public LogicalGraphSnapshotImpl( Map<Long, LogicalGraphCatalog> namespaces ) {
        this.graphs = buildGraphIds( namespaces );
        this.graphNames = buildGraphNames( namespaces );
    }


    private ImmutableMap<String, LogicalGraph> buildGraphNames( Map<Long, LogicalGraphCatalog> namespaces ) {
        return ImmutableMap.copyOf( graphs.values().stream().collect( Collectors.toMap( g -> g.name, g -> g ) ) );
    }


    private ImmutableMap<Long, LogicalGraph> buildGraphIds( Map<Long, LogicalGraphCatalog> namespaces ) {
        return ImmutableMap.copyOf( namespaces.entrySet().stream().collect( Collectors.toMap( Entry::getKey, n -> n.getValue().getGraphs().get( n.getKey() ) ) ) );
    }


    @Override
    public @NonNull Optional<LogicalGraph> getGraph( long id ) {
        return Optional.ofNullable( graphs.get( id ) );
    }


    @Override
    public @NonNull List<LogicalGraph> getGraphs( @Nullable Pattern graphName ) {
        List<LogicalGraph> graphs = List.copyOf( this.graphs.values() );

        if ( graphName == null ) {
            return graphs;
        }

        return graphNames.values().stream().filter( g -> g.caseSensitive ? g.name.matches( graphName.toRegex() ) : g.name.toLowerCase().matches( graphName.toRegex().toLowerCase() ) ).collect( Collectors.toList() );
    }


}
