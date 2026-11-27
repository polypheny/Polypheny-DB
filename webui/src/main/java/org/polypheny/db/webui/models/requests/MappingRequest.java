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
import org.polypheny.db.ddl.DocumentMappingUpdate;
import org.polypheny.db.ddl.RelMappingUpdate;


public final class MappingRequest {

    private MappingRequest() {
    }


    public record ListRequest( @JsonProperty(value = "adapterIds", required = true) List<Long> adapterIds ) {
    }


    public record OverviewRequest( Long adapterId, Long targetEntityId ) {
    }


    public record MappingIdRequest( @JsonProperty(value = "mappingId", required = true) long mappingId ) {
    }


    public record RelDraftRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                 @JsonProperty(value = "targetEntityId", required = true) long targetEntityId, Long existingTargetEntityId, Long sourceAllocationId ) {
    }


    public record RemoveRequest( @JsonProperty(value = "mappingId", required = true) long mappingId,
                                 @JsonProperty(value = "expectedRevision", required = true) String expectedRevision ) {
    }


    public record RelCreateRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                 @JsonProperty(value = "targetEntityId", required = true) long targetEntityId,
                                 @JsonProperty(value = "expectedRevision", required = true) String expectedRevision,
                                 @JsonProperty(value = "mappingName", required = true) String mappingName,
                                 @JsonProperty(value = "logicalTableName", required = true) String logicalTableName,
                                 @JsonProperty(value = "columns", required = true) List<RelMappingUpdate.ColumnName> columns,
                                 Long targetNamespaceId, Long existingTargetEntityId, Long sourceAllocationId ) {

        public RelMappingUpdate toCommand() {
            return new RelMappingUpdate( adapterId, -1, expectedRevision, mappingName, logicalTableName, columns, targetNamespaceId, existingTargetEntityId, sourceAllocationId );
        }

    }


    public record DetailRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                 @JsonProperty(value = "mappingId", required = true) long mappingId ) {
    }


    public record RelValidationRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                     @JsonProperty(value = "expectedRevision", required = true) String expectedRevision,
                                     @JsonProperty(value = "columns", required = true) List<RelMappingUpdate.ColumnName> columns,
                                     Long mappingId, Long targetEntityId, Long existingTargetEntityId, Long sourceAllocationId ) {
    }


    public record RelUpdateRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                 @JsonProperty(value = "mappingId", required = true) long mappingId,
                                 @JsonProperty(value = "expectedRevision", required = true) String expectedRevision,
                                 @JsonProperty(value = "mappingName", required = true) String mappingName,
                                 @JsonProperty(value = "logicalTableName", required = true) String logicalTableName,
                                 @JsonProperty(value = "columns", required = true) List<RelMappingUpdate.ColumnName> columns,
                                 Long targetNamespaceId ) {

        public RelMappingUpdate toCommand() {
            return new RelMappingUpdate( adapterId, mappingId, expectedRevision, mappingName, logicalTableName, columns, targetNamespaceId );
        }

    }


    public record DocumentDraftRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                        @JsonProperty(value = "sourceEntityId", required = true) long sourceEntityId, Long sourceAllocationId, Long existingTargetEntityId ) {
    }


    public record DocumentCreateRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                         @JsonProperty(value = "sourceEntityId", required = true) long sourceEntityId,
                                         @JsonProperty(value = "expectedRevision", required = true) String expectedRevision,
                                         @JsonProperty(value = "mappingName", required = true) String mappingName,
                                         @JsonProperty(value = "logicalCollectionName", required = true) String logicalCollectionName,
                                         @JsonProperty(value = "targetNamespaceId", required = true) long targetNamespaceId,
                                         Long sourceAllocationId, Long existingTargetEntityId ) {

        public DocumentMappingUpdate toCommand() {
            return new DocumentMappingUpdate( adapterId, -1, expectedRevision, mappingName, logicalCollectionName,
                    targetNamespaceId, existingTargetEntityId, sourceAllocationId );
        }

    }


    public record DocumentUpdateRequest( @JsonProperty(value = "adapterId", required = true) long adapterId,
                                         @JsonProperty(value = "mappingId", required = true) long mappingId,
                                         @JsonProperty(value = "expectedRevision", required = true) String expectedRevision,
                                         @JsonProperty(value = "mappingName", required = true) String mappingName,
                                         @JsonProperty(value = "logicalCollectionName", required = true) String logicalCollectionName,
                                         Long targetNamespaceId ) {

        public DocumentMappingUpdate toCommand() {
            return new DocumentMappingUpdate( adapterId, mappingId, expectedRevision, mappingName, logicalCollectionName, targetNamespaceId, null, null );
        }

    }

}
