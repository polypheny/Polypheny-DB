/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.webui.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.polypheny.db.ddl.MappingUpdate;


public final class MappingRequest {

    private MappingRequest() {
    }


    public record ListRequest( @JsonProperty(value = "adapterIds", required = true) List<Long> adapterIds ) {
    }


    public record DetailRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                 @JsonProperty(value = "mappingId", required = true) long mappingId ) {
    }


    public record UpdateRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                 @JsonProperty(value = "mappingId", required = true) long mappingId,
                                 @JsonProperty(value = "expectedRevision", required = true) String expectedRevision,
                                 @JsonProperty(value = "mappingName", required = true) String mappingName,
                                 @JsonProperty(value = "logicalTableName", required = true) String logicalTableName,
                                 @JsonProperty(value = "columns", required = true) List<MappingUpdate.ColumnName> columns ) {

        public MappingUpdate toCommand() {
            return new MappingUpdate( adapterId, mappingId, expectedRevision, mappingName, logicalTableName, columns );
        }

    }

}
