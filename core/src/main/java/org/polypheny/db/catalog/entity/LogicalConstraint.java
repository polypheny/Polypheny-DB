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

package org.polypheny.db.catalog.entity;


import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.logistic.ConstraintType;


@Value
public class LogicalConstraint implements Serializable, Comparable<LogicalConstraint> {

    @Serialize
    @JsonProperty
    public long id;
    @Serialize
    @JsonProperty
    public long keyId;
    @Serialize
    @JsonProperty
    public ConstraintType type;
    @Serialize
    @JsonProperty
    public String name;
    @Serialize
    @JsonProperty
    public LogicalKey key;


    public LogicalConstraint(
            @Deserialize("id") final long id,
            @Deserialize("keyId") final long keyId,
            @Deserialize("type") @NonNull final ConstraintType constraintType,
            @Deserialize("name") final String name,
            @Deserialize("key") final LogicalKey key ) {
        this.id = id;
        this.keyId = keyId;
        this.type = constraintType;
        this.name = name;
        this.key = key;
    }


    @Override
    public int compareTo( @NotNull LogicalConstraint o ) {
        if ( this == o ) {
            return 0;
        }
        return Long.compare( id, o.id );
    }

}
