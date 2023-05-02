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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;

@Slf4j
public class LogicalDocSnapshotImpl implements LogicalDocSnapshot {

    private final ImmutableMap<Long, LogicalNamespace> namespaces;
    private final ImmutableMap<Long, LogicalCollection> collections;
    private final ImmutableMap<String, LogicalCollection> collectionNames;
    private final ImmutableMap<Long, List<LogicalCollection>> namespaceCollections;


    public LogicalDocSnapshotImpl( Map<Long, LogicalDocumentCatalog> catalogs ) {
        this.namespaces = ImmutableMap.copyOf( catalogs.values().stream().collect( Collectors.toMap( n -> n.getLogicalNamespace().id, n -> n.getLogicalNamespace() ) ) );
        this.collections = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getCollections().values().stream() ).collect( Collectors.toMap( c -> c.id, c -> c ) ) );
        this.collectionNames = ImmutableMap.copyOf( this.collections.values().stream().collect( Collectors.toMap( c -> c.name, c -> c ) ) );
        this.namespaceCollections = ImmutableMap.copyOf( catalogs.values().stream().collect( Collectors.toMap( c -> c.getLogicalNamespace().id, c -> List.copyOf( c.getCollections().values() ) ) ) );
    }


    @Override
    public LogicalCollection getCollection( long id ) {
        return collections.get( id );
    }


    @Override
    public List<LogicalCollection> getCollections( long namespaceId, Pattern namePattern ) {
        List<LogicalCollection> collections = namespaceCollections.get( namespaceId );

        if ( namePattern == null ) {
            return collections;
        }

        return collections.stream().filter( c -> namespaces.get( c.namespaceId ).caseSensitive
                ? c.name.matches( namePattern.toRegex() )
                : c.name.toLowerCase().matches( namePattern.toLowerCase().toRegex() ) ).collect( Collectors.toList() );
    }


    @Override
    public LogicalCollection getCollection( long namespaceId, String name ) {
        List<LogicalCollection> collections = namespaceCollections.get( namespaceId );

        return collections.stream().filter( c -> namespaces.get( c.namespaceId ).caseSensitive
                ? c.name.equals( name )
                : c.name.equalsIgnoreCase( name ) ).findFirst().orElse( null );
    }

}
