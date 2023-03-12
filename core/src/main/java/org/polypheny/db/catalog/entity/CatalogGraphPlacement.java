/*
 * Copyright 2019-2022 The Polypheny Project
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

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;


@EqualsAndHashCode(callSuper = true)
@Value
@SuperBuilder(toBuilder = true)
public class CatalogGraphPlacement extends CatalogEntityPlacement {

    private static final long serialVersionUID = 5889825050034392549L;

    @Serialize
    public long adapterId;
    @Serialize
    public long graphId;
    @Serialize
    public String physicalName;
    @Serialize
    public long partitionId;


    public CatalogGraphPlacement(
            @Deserialize("adapterId") long adapterId,
            @Deserialize("graphId") long graphId,
            @Deserialize("physicalName") @Nullable String physicalName,
            @Deserialize("partitionId") long partitionId ) {
        super( graphId, adapterId, graphId );
        this.adapterId = adapterId;
        this.graphId = graphId;
        this.physicalName = physicalName;
        this.partitionId = partitionId;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


}
