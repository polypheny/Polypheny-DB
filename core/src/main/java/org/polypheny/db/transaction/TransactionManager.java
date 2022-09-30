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

package org.polypheny.db.transaction;


import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;


public interface TransactionManager {

    Transaction startTransaction( CatalogUser user, CatalogSchema defaultSchema, CatalogDatabase database, boolean analyze, String origin );

    Transaction startTransaction( CatalogUser user, CatalogSchema defaultSchema, CatalogDatabase database, boolean analyze, String origin, MultimediaFlavor flavor );

    Transaction startTransaction( long userId, long databaseId, boolean analyze, String origin ) throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownSchemaException;

    Transaction startTransaction( long userId, long databaseId, boolean analyze, String origin, MultimediaFlavor flavor ) throws UnknownUserException, UnknownDatabaseException, UnknownSchemaException;

    void removeTransaction( PolyXid xid );

    boolean isActive( PolyXid xid );

}
