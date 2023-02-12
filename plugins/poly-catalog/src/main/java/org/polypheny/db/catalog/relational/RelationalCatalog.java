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

package org.polypheny.db.catalog.relational;

import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.ModelCatalog;

public class RelationalCatalog implements ModelCatalog {

    private Map<Long, CatalogSchema> schemas = new HashMap<>();

    private Map<Long, CatalogTable> tables = new HashMap<>();

    private Map<Long, CatalogColumn> columns = new HashMap<>();

    private boolean openChanges = false;


    @Override
    public void commit() {

        openChanges = false;
    }


    @Override
    public void rollback() {

        openChanges = false;
    }


    @Override
    public boolean hasUncommitedChanges() {
        return openChanges;
    }


    public void addSchema( long id, String name, long databaseId, NamespaceType namespaceType ) {
    }


    public void addTable( long id, String name, long namespaceId ) {
    }


    public void addColumn( long id, String name, long entityId, AlgDataType type ) {
    }

}
