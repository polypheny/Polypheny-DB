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

package org.polypheny.db.backup.dependencies;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.util.Pair;

/**
 * Class that contains entities and by what they are referenced
 */
public class EntityReferencer {
    @Getter
    private Long entityId;
    @Getter
    private BackupEntityType entityType;
    @Getter @Setter
    private List<Long> referencerNamespaces;    //TODO(FF): is self included?
    @Getter @Setter
    private List<Long> referencerTables;
    @Getter @Setter
    private List<Pair<Long, Long>> referencerNamespaceTablePairs;


    /**
     * Creates an entityReferencer, which contains the id and type of the entity and the ids of the entities that reference it ("namespace", "table" are the only implemented ones at the moment)
     * @param entityId The id of the entity that is referenced by another entity
     * @param entityType The type of the entity
     */
    public EntityReferencer(Long entityId, BackupEntityType entityType) {
        this.entityId = entityId;
        this.entityType = entityType;
    }


    /**
     * Checks if the entity is referenced by another entity
     * @param entityId The id of the entity you want to check whether it is referenced by another entity
     * @param entityType The type of the entity to be checked
     * @return true if the entity is referenced by another entity, false if not
     */
    public Boolean isReferenced(Long entityId, BackupEntityType entityType) {
        if (referencerNamespaces.isEmpty() && referencerTables.isEmpty()) {
            return false;
        }
        else {
            return true;
        }
    }

}
