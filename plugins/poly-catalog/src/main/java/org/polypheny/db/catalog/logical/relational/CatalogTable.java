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

package org.polypheny.db.catalog.logical.relational;

import com.google.common.collect.ImmutableMap;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import lombok.Value;
import lombok.With;

@Value
public class CatalogTable {

    @Serialize
    @With
    public long id;

    @Serialize
    @With
    public String name;

    @Serialize
    @With
    public long namespaceId;


    @Serialize
    @With
    public ImmutableMap<Long, CatalogColumn> columns;


    public CatalogTable( long id, String name, long namespaceId ) {
        this( id, name, namespaceId, new HashMap<>() );
    }


    public CatalogTable(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("columns") Map<Long, CatalogColumn> columns ) {
        this.id = id;
        this.name = name;
        this.namespaceId = namespaceId;
        this.columns = ImmutableMap.copyOf( columns );
    }


    public CatalogTable withAddedColumn( long id, String name ) {
        Map<Long, CatalogColumn> columns = new HashMap<>( this.columns );
        columns.put( id, new CatalogColumn( id, name, this.id ) );
        return withColumns( ImmutableMap.copyOf( columns ) );
    }


    public CatalogTable withDeletedColumn( long id ) {
        Map<Long, CatalogColumn> columns = new HashMap<>( this.columns );
        columns.remove( id );
        return withColumns( ImmutableMap.copyOf( columns ) );
    }

}
