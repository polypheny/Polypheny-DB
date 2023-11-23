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
import org.polypheny.db.catalog.entity.LogicalObject;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;

@Getter @Setter
public class BackupEntityWrapper<E extends LogicalObject> {

    private E entityObject;

    private Boolean toBeInserted = true;

    //default, original name (change if rename needed (options))
    private String nameForQuery;

    private EntityReferencer entityReferencer;


    public BackupEntityWrapper( E entity, Boolean toBeInserted, String nameForQuery, EntityReferencer entityReferencer ) {
        this.entityObject = entity;
        this.toBeInserted = toBeInserted;
        this.nameForQuery = nameForQuery;
        this.entityReferencer = entityReferencer;
    }

    public BackupEntityWrapper( E entity, String nameForQuery, EntityReferencer entityReferencer ) {
        this.entityObject = entity;
        this.nameForQuery = nameForQuery;
        this.entityReferencer = entityReferencer;
    }

}
