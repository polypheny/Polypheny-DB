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

package org.polypheny.db.catalog.catalogs;

import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;


public interface LogicalCatalog {

    /**
     * Checks if there is a table with the specified name in the specified schema.
     *
     * @param entityName The name to check for
     * @return true if there is a table with this name, false if not.
     */
    public abstract boolean checkIfExistsEntity( String entityName );

    /**
     * Checks if there is a table with the specified id.
     *
     * @param tableId id of the table
     * @return true if there is a table with this id, false if not.
     */
    public abstract boolean checkIfExistsEntity( long tableId );

    LogicalNamespace getLogicalNamespace();


    LogicalEntity getEntity( String name );

    LogicalEntity getEntity( long id );

    LogicalCatalog withLogicalNamespace( LogicalNamespace namespace );

}
