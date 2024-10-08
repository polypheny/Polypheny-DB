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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.io.Serial;
import java.util.List;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.snapshot.Snapshot;


@Value
@NonFinal
@SerializeClass(subclasses = { LogicalGenericKey.class, LogicalPrimaryKey.class, LogicalForeignKey.class })
@JsonTypeInfo(use = Id.CLASS)
public abstract class LogicalKey implements PolyObject, Comparable<LogicalKey> {

    @Serial
    private static final long serialVersionUID = -5803762884192662540L;

    @Serialize
    @JsonProperty
    public long id;

    @Serialize
    @JsonProperty
    public long entityId;

    @Serialize
    @JsonProperty
    public long namespaceId;

    @Serialize
    @JsonProperty
    public ImmutableList<Long> fieldIds;

    @Serialize
    @JsonProperty
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
        return fieldIds.stream().map( columnId -> snapshot.rel().getColumn( columnId ).orElseThrow().name ).toList();
    }


    @Override
    public int compareTo( @NotNull LogicalKey o ) {
        return Long.compare( this.id, o.id );
    }


    public enum EnforcementTime {
        ON_COMMIT,
        ON_QUERY
    }


}
