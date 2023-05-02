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

package org.polypheny.db.languages.mql;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
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
        AdapterManager adapterManager = AdapterManager.getInstance();

        long namespaceId = context.getSnapshot().getNamespace( ((MqlQueryParameters) parameters).getDatabase() ).id;

        LogicalCollection collection = context.getSnapshot().doc().getCollection( namespaceId, getCollection() );

        if ( collection == null ) {
            throw new RuntimeException( "Error while adding new collection placement, collection not found." );
        }

        List<DataStore<?>> dataStores = stores
                .stream()
                .map( store -> (DataStore<?>) adapterManager.getAdapter( store ) )
                .collect( Collectors.toList() );

        if ( statement.getTransaction().getSnapshot().alloc().getFromLogical( collection.id ).stream().noneMatch( p -> dataStores.stream().map( Adapter::getAdapterId ).collect( Collectors.toList() ).contains( p.adapterId ) ) ) {
            throw new RuntimeException( "Error while adding a new collection placement, placement already present." );
        }

        DdlManager.getInstance().dropCollectionAllocation( namespaceId, collection, dataStores, statement );
    }


    @Override
    public Type getMqlKind() {
        return Type.DELETE_PLACEMENT;
    }

}
