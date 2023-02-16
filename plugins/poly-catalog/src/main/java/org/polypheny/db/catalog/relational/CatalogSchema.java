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

package org.polypheny.db.catalog.relational;

import com.google.common.collect.ImmutableMap;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.Map;
import lombok.Value;

@Value
public class CatalogSchema {

    @Serialize
    public long id;

    @Serialize
    public String name;

    @Serialize
    public long databaseId;

    @Serialize
    public ImmutableMap<Long, CatalogTable> tables;


    public CatalogSchema(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("databaseId") long databaseId,
            @Deserialize("tables") Map<Long, CatalogTable> tables ) {
        this.id = id;
        this.name = name;
        this.databaseId = databaseId;
        this.tables = ImmutableMap.copyOf( tables );
    }


    public CatalogSchema addTable( CatalogTable catalogTable ) {
        Map<Long, CatalogTable> newTables = new HashMap<>( tables );
        newTables.put( catalogTable.id, catalogTable );
        return new CatalogSchema( id, name, databaseId, newTables );
    }

}
