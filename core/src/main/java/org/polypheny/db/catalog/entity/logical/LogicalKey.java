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
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.io.Serial;
import java.util.LinkedList;
import java.util.List;
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
    public ImmutableList<Long> columnIds;

    @Serialize
    public EnforcementTime enforcementTime;


    public LogicalKey( long id, long entityId, long namespaceId, List<Long> columnIds, EnforcementTime enforcementTime ) {
        this.id = id;
        this.entityId = entityId;
        this.namespaceId = namespaceId;
        this.columnIds = ImmutableList.copyOf( columnIds );
        this.enforcementTime = enforcementTime;
    }


    public String getSchemaName() {
        return Catalog.snapshot().getNamespace( namespaceId ).orElseThrow().name;
    }


    public String getTableName() {
        return Catalog.snapshot().rel().getTable( entityId ).orElseThrow().name;
    }


    public List<String> getColumnNames() {
        Snapshot snapshot = Catalog.snapshot();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : columnIds ) {
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


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }

        LogicalKey that = (LogicalKey) o;

        if ( id != that.id ) {
            return false;
        }
        if ( entityId != that.entityId ) {
            return false;
        }
        if ( namespaceId != that.namespaceId ) {
            return false;
        }
        if ( !columnIds.equals( that.columnIds ) ) {
            return false;
        }
        return enforcementTime == that.enforcementTime;
    }


    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (entityId ^ (entityId >>> 32));
        result = 31 * result + (int) (namespaceId ^ (namespaceId >>> 32));
        result = 31 * result + columnIds.hashCode();
        result = 31 * result + (enforcementTime != null ? enforcementTime.hashCode() : 0);
        return result;
    }

}
