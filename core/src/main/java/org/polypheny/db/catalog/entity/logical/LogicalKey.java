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

package org.polypheny.db.catalog.entity.logical;

import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.snapshot.Snapshot;


@EqualsAndHashCode
@Value
@NonFinal
public class LogicalKey implements CatalogObject, Comparable<LogicalKey> {

    private static final long serialVersionUID = -5803762884192662540L;

    @Serialize
    public long id;
    @Serialize
    public long tableId;
    @Serialize
    public long namespaceId;
    @Serialize
    public ImmutableList<Long> columnIds;
    @Serialize
    public EnforcementTime enforcementTime;


    public LogicalKey(
            @Deserialize("id") final long id,
            @Deserialize("tableId") final long tableId,
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("columnIds") final List<Long> columnIds,
            @Deserialize("enforcementTime") EnforcementTime enforcementTime ) {
        this.id = id;
        this.tableId = tableId;
        this.namespaceId = namespaceId;
        this.columnIds = ImmutableList.copyOf( columnIds );
        this.enforcementTime = enforcementTime;
    }


    @SneakyThrows
    public String getSchemaName() {
        return Catalog.snapshot().getNamespace( namespaceId ).name;
    }


    @SneakyThrows
    public String getTableName() {
        return Catalog.snapshot().rel().getTable( tableId ).name;
    }


    @SneakyThrows
    public List<String> getColumnNames() {
        Snapshot snapshot = Catalog.snapshot();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : columnIds ) {
            columnNames.add( snapshot.rel().getColumn( columnId ).name );
        }
        return columnNames;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ id, tableId, getTableName(), namespaceId, getSchemaName(), null, null };
    }


    @Override
    public int compareTo( @NotNull LogicalKey o ) {
        return (int) (this.id - o.id);
    }


    public enum EnforcementTime {
        ON_COMMIT,
        ON_QUERY
    }

}
