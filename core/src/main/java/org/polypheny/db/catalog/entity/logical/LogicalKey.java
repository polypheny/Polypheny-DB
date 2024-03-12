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

package org.polypheny.db.catalog.entity.logical;

import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@Value
@NonFinal
@EqualsAndHashCode
@SerializeClass(subclasses = { LogicalGenericKey.class, LogicalPrimaryKey.class, LogicalForeignKey.class })
public abstract class LogicalKey implements PolyObject, Comparable<LogicalKey> {

    @Serial
    private static final long serialVersionUID = -5803762884192662540L;

    @Serialize
    public long id;

    @Serialize
    public long entityId;

    @Serialize
    public long namespaceId;

    @Serialize
    public ImmutableList<Long> fieldIds;

    @Serialize
    public EnforcementTime enforcementTime;


    public LogicalKey( long id, long entityId, long namespaceId, List<Long> fieldIds, EnforcementTime enforcementTime ) {
        this.id = id;
        this.entityId = entityId;
        this.namespaceId = namespaceId;
        this.fieldIds = ImmutableList.copyOf( fieldIds );
        this.enforcementTime = enforcementTime;
    }


    public String getSchemaName() {
        return Catalog.snapshot().getNamespace( namespaceId ).orElseThrow().name;
    }


    public String getTableName() {
        return Catalog.snapshot().rel().getTable( entityId ).orElseThrow().name;
    }


    public List<String> getFieldNames() {
        Snapshot snapshot = Catalog.snapshot();
        List<String> columnNames = new ArrayList<>();
        for ( long columnId : fieldIds ) {
            columnNames.add( snapshot.rel().getColumn( columnId ).orElseThrow().name );
        }
        return columnNames;
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[]{ PolyLong.of( id ), PolyLong.of( entityId ), PolyString.of( getTableName() ), PolyLong.of( namespaceId ), PolyString.of( getSchemaName() ), null, null };
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
