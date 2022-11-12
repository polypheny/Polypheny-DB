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

package org.polypheny.db.cypher.ddl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.admin.CypherAdminCommand;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


public class CypherAddPlacement extends CypherAdminCommand implements ExecutableStatement {

    private final String store;
    private final String database;


    public CypherAddPlacement(
            ParserPos pos,
            CypherSimpleEither<String, CypherParameter> databaseName,
            CypherSimpleEither<String, CypherParameter> storeName ) {
        super( pos );
        this.database = getNameOrNull( databaseName );
        this.store = getNameOrNull( storeName );

        if ( this.database == null ) {
            throw new RuntimeException( "Unknown database name." );
        }
        if ( this.store == null ) {
            throw new RuntimeException( "Unknown store name." );
        }
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        Catalog catalog = Catalog.getInstance();
        AdapterManager adapterManager = AdapterManager.getInstance();

        List<CatalogGraphDatabase> graphs = catalog.getGraphs( Catalog.defaultDatabaseId, new Pattern( this.database ) );

        List<DataStore> dataStores = Stream.of( store )
                .map( store -> (DataStore) adapterManager.getAdapter( store ) )
                .collect( Collectors.toList() );

        if ( !adapterManager.getAdapters().containsKey( store ) ) {
            throw new RuntimeException( "The targeted store does not exist." );
        }

        if ( graphs.size() != 1 ) {
            throw new RuntimeException( "Error while adding graph placement." );
        }

        if ( graphs.get( 0 ).placements.stream().anyMatch( p -> dataStores.stream().map( Adapter::getAdapterId ).collect( Collectors.toList() ).contains( p ) ) ) {
            throw new RuntimeException( "Could not create placement of graph as it already exists." );
        }

        DdlManager.getInstance().addGraphPlacement( graphs.get( 0 ).id, dataStores, true, statement );
    }

}
