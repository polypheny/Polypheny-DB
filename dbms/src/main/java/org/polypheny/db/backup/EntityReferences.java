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

package org.polypheny.db.backup;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.backup.BackupEntityType;

/**
 * Class that contains entities and by what they are referenced
 */
public class EntityReferences {
    private Long entityId;
    private BackupEntityType entityType;
    @Getter @Setter
    private List<Long> referencerNamespaces;
    @Getter @Setter
    private List<Long> referencerTables;


    /**
     *
     * @param entityId The id of the entity that is referenced by another entity
     * @param entityType The type of the entity ("namespace", "
     */
    public EntityReferences(Long entityId, BackupEntityType entityType) {
        this.entityId = entityId;
        this.entityType = entityType;
    }

    public Boolean isReferenced(Long entityId, BackupEntityType entityType) {
        if (referencerNamespaces.isEmpty() && referencerTables.isEmpty()) {
            return false;
        }
        else {
            return true;
        }
    }

}
