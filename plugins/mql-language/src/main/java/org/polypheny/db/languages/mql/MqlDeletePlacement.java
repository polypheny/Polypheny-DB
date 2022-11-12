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

package org.polypheny.db.languages.mql;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;

public class MqlDeletePlacement extends MqlCollectionStatement implements ExecutableStatement {

    public MqlDeletePlacement( ParserPos pos, String collection, List<String> stores ) {
        super( collection, pos );
        this.stores = stores;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        final Catalog catalog = Catalog.getInstance();
        AdapterManager adapterManager = AdapterManager.getInstance();

        long namespaceId;
        try {
            namespaceId = catalog.getSchema( Catalog.defaultDatabaseId, ((MqlQueryParameters) parameters).getDatabase() ).id;
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "The used document database (Polypheny Schema) is not available." );
        }

        List<CatalogCollection> collections = catalog.getCollections( namespaceId, new Pattern( getCollection() ) );

        if ( collections.size() != 1 ) {
            throw new RuntimeException( "Error while adding new collection placement, collection not found." );
        }

        List<DataStore> dataStores = stores
                .stream()
                .map( store -> (DataStore) adapterManager.getAdapter( store ) )
                .collect( Collectors.toList() );

        if ( collections.get( 0 ).placements.stream().noneMatch( p -> dataStores.stream().map( Adapter::getAdapterId ).collect( Collectors.toList() ).contains( p ) ) ) {
            throw new RuntimeException( "Error while adding a new collection placement, placement already present." );
        }

        DdlManager.getInstance().dropCollectionPlacement( namespaceId, collections.get( 0 ), dataStores, statement );
    }


    @Override
    public Type getMqlKind() {
        return Type.DELETE_PLACEMENT;
    }

}
