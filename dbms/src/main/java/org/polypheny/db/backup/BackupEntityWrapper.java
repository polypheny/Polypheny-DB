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

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.backup.dependencies.EntityReferencer;
import org.polypheny.db.catalog.entity.PolyObject;

/**
 * Wrapps elements to be backed up with additional information needed for the insertion of the backup process
 * @param <E> The type of element to be wrapped
 */
@Getter @Setter
public class BackupEntityWrapper<E extends PolyObject> {

    private E entityObject;

    private Boolean toBeInserted = true;

    //default, original name (change if rename needed (options))
    private String nameForQuery;

    private EntityReferencer entityReferencer;


    /**
     * Constructor for a BackupEntityWrapper
     * @param entity The entity to be wrapped
     * @param toBeInserted Whether the entity should be inserted or not (on restoration, default true)
     * @param nameForQuery The name to be used for the entity in the restoration (insertion), the original name is preserved in the entityObject
     * @param entityReferencer The entityReferencer to be used for the entity (all entities that reference this entity)
     */
    public BackupEntityWrapper( E entity, Boolean toBeInserted, String nameForQuery, EntityReferencer entityReferencer ) {
        this.entityObject = entity;
        this.toBeInserted = toBeInserted;
        this.nameForQuery = nameForQuery;
        this.entityReferencer = entityReferencer;
    }


    /**
     * Constructor for a BackupEntityWrapper
     * @param entity The entity to be wrapped
     * @param nameForQuery The name to be used for the entity in the restoration (insertion), the original name is preserved in the entityObject
     * @param entityReferencer The entityReferencer to be used for the entity (all entities that reference this entity)
     */
    public BackupEntityWrapper( E entity, String nameForQuery, EntityReferencer entityReferencer ) {
        this.entityObject = entity;
        this.nameForQuery = nameForQuery;
        this.entityReferencer = entityReferencer;
    }

}
