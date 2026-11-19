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

package org.polypheny.db.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.polypheny.db.type.PolyType;


/**
 * Backend command object carrying the user’s requested mapping changes.
 * */
public record MappingUpdate(
        long adapterId,
        long mappingId,
        String expectedRevision,
        String mappingName,
        String logicalTableName,
        List<ColumnName> columns,
        Long targetNamespaceId,
        Long existingTargetEntityId,
        Long sourceAllocationId ) {

    public MappingUpdate( long adapterId, long mappingId, String expectedRevision, String mappingName, String logicalTableName,
            List<ColumnName> columns, Long targetNamespaceId ) {
        this( adapterId, mappingId, expectedRevision, mappingName, logicalTableName, columns, targetNamespaceId, null, null );
    }

    public MappingUpdate( long adapterId, long mappingId, String expectedRevision, String mappingName, String logicalTableName, List<ColumnName> columns ) {
        this( adapterId, mappingId, expectedRevision, mappingName, logicalTableName, columns, null );
    }

    public record ColumnName( @JsonProperty(value = "logicalColumnId", required = true) long logicalColumnId,
                              @JsonProperty(value = "logicalName", required = true) String logicalName,
                              Boolean included, PolyType targetType, Boolean targetNullable, Integer targetPrecision, Integer targetScale, String ruleType ) {

        public ColumnName( long logicalColumnId, String logicalName, Boolean included, PolyType targetType, Boolean targetNullable,
                Integer targetPrecision, Integer targetScale ) {
            this( logicalColumnId, logicalName, included, targetType, targetNullable, targetPrecision, targetScale, null );
        }

        public ColumnName( long logicalColumnId, String logicalName, Boolean included, PolyType targetType, Boolean targetNullable ) {
            this( logicalColumnId, logicalName, included, targetType, targetNullable, null, null );
        }

        public ColumnName( long logicalColumnId, String logicalName, Boolean included ) {
            this( logicalColumnId, logicalName, included, null, null );
        }

        public ColumnName( long logicalColumnId, String logicalName ) {
            this( logicalColumnId, logicalName, null );
        }

        public boolean changesTypeOrNullability() {
            return targetType != null || targetNullable != null;
        }
    }

}
