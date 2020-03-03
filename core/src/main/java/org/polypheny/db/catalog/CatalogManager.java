/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.catalog;


import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.transaction.PolyXid;


public abstract class CatalogManager {

    /**
     * Returns the user with the specified name.
     *
     * @param userName The name of the database
     * @return The user
     * @throws UnknownUserException If there is no user with this name.
     */
    public abstract CatalogUser getUser( String userName ) throws UnknownUserException, GenericCatalogException;

    public abstract Catalog getCatalog( PolyXid xid );

    public abstract Catalog getCatalog();
}
