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
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;


@Value
@EqualsAndHashCode(callSuper = false)
public class LogicalPrimaryKey extends LogicalKey {

    @Serialize
    public LogicalKey key;


    public LogicalPrimaryKey( @Deserialize("key") @NonNull final LogicalKey key ) {
        super(
                key.id,
                key.entityId,
                key.namespaceId,
                key.fieldIds,
                EnforcementTime.ON_QUERY );

        this.key = key;
    }

}
