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


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import java.io.Serial;
import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.logistic.IndexType;


@Value
@SuperBuilder(toBuilder = true)
public class LogicalIndex implements Serializable {

    @Serial
    private static final long serialVersionUID = -318228681682792406L;

    @Serialize
    public long id;
    @Serialize
    public String name;
    @Serialize
    @SerializeNullable
    public String physicalName;
    @Serialize
    public boolean unique;
    @Serialize
    public IndexType type;
    @Serialize
    public long location; // -1 is Polypheny
    @Serialize
    public String method;
    @Serialize
    public String methodDisplayName;
    @Serialize
    public LogicalKey key;
    @Serialize
    public long keyId;


    public LogicalIndex(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("unique") final boolean unique,
            @Deserialize("method") final String method,
            @Deserialize("methodDisplayName") final String methodDisplayName,
            @Deserialize("type") final IndexType type,
            @Deserialize("location") final Long location,
            @Deserialize("keyId") final long keyId,
            @Deserialize("key") final LogicalKey key,
            @Deserialize("physicalName") final String physicalName ) {
        this.id = id;
        this.name = name;
        this.unique = unique;
        this.method = method;
        this.methodDisplayName = methodDisplayName;
        this.type = type;
        this.location = location;
        this.keyId = keyId;
        this.key = key;
        this.physicalName = physicalName;
    }

}
