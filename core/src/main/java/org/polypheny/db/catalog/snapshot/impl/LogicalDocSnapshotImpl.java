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
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;

@Slf4j
@EqualsAndHashCode
public class LogicalDocSnapshotImpl implements LogicalDocSnapshot {

    private final ImmutableMap<Long, LogicalNamespace> namespaces;
    private final ImmutableMap<Long, LogicalCollection> collections;
    private final ImmutableMap<String, LogicalCollection> collectionNames;
    private final ImmutableMap<Long, List<LogicalCollection>> namespaceCollections;


    public LogicalDocSnapshotImpl( Map<Long, LogicalDocumentCatalog> catalogs ) {
        this.namespaces = ImmutableMap.copyOf( catalogs.values().stream().collect( Collectors.toMap( n -> n.getLogicalNamespace().id, LogicalCatalog::getLogicalNamespace ) ) );
        this.collections = ImmutableMap.copyOf( catalogs.values().stream().flatMap( c -> c.getCollections().values().stream() ).collect( Collectors.toMap( c -> c.id, c -> c ) ) );
        this.collectionNames = ImmutableMap.copyOf( this.collections.values().stream().collect( Collectors.toMap( c -> c.name, c -> c ) ) );
        this.namespaceCollections = ImmutableMap.copyOf( catalogs.values().stream().collect( Collectors.toMap( c -> c.getLogicalNamespace().id, c -> List.copyOf( c.getCollections().values() ) ) ) );
    }


    @Override
    public @NonNull Optional<LogicalCollection> getCollection( long id ) {
        return Optional.ofNullable( collections.get( id ) );
    }


    @Override
    public @NonNull List<LogicalCollection> getCollections( long namespaceId, @Nullable Pattern namePattern ) {
        List<LogicalCollection> collections = Optional.ofNullable( namespaceCollections.get( namespaceId ) ).orElse( List.of() );

        if ( namePattern == null ) {
            return collections;
        }

        return collections.stream().filter( c -> namespaces.get( c.namespaceId ).caseSensitive
                ? c.name.matches( namePattern.toRegex() )
                : c.name.toLowerCase().matches( namePattern.toLowerCase().toRegex() ) ).collect( Collectors.toList() );
    }


    @Override
    public @NonNull List<LogicalCollection> getCollections( @Nullable Pattern namespacePattern, @Nullable Pattern namePattern ) {
        List<LogicalNamespace> namespaces = this.namespaces.values().asList();

        if ( namespacePattern != null ) {
            namespaces = namespaces.stream().filter( n -> n.caseSensitive
                    ? n.name.matches( namespacePattern.toRegex() )
                    : n.name.toLowerCase().matches( namespacePattern.toLowerCase().toRegex() ) ).toList();
        }

        return namespaces.stream().flatMap( n -> getCollections( n.id, namePattern ).stream() ).collect( Collectors.toList() );
    }


    @Override
    public @NonNull Optional<LogicalCollection> getCollection( long namespaceId, String name ) {
        List<LogicalCollection> collections = Optional.ofNullable( namespaceCollections.get( namespaceId ) ).orElse( List.of() );

        return collections.stream().filter( c -> namespaces.get( c.namespaceId ).caseSensitive
                ? c.name.equals( name )
                : c.name.equalsIgnoreCase( name ) ).findFirst();
    }



}
