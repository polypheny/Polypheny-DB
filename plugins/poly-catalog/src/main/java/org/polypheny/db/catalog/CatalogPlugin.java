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

package org.polypheny.db.catalog;

import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.logistic.NamespaceType;

public class CatalogPlugin extends Plugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to
     * be successfully loaded by manager.
     *
     * @param wrapper
     */
    public CatalogPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        PolyCatalog catalog = new PolyCatalog();

        catalog.addNamespace( "test", NamespaceType.RELATIONAL );
        long namespaceId = catalog.addNamespace( "test2", NamespaceType.RELATIONAL );

        long tableId = catalog.addTable( "testTable", namespaceId );
        catalog.addColumn( "testColumn", namespaceId, tableId, null );
        catalog.commit();

        byte[] buffer = catalog.serialize();

        PolyCatalog catalog1 = catalog.deserialize( buffer, PolyCatalog.class );

        catalog1.addColumn( "testColumn2", namespaceId, tableId, null );
        catalog1.rollback();

    }

}
