/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.stream.Stream;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.admin.CypherAdminCommand;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;

public class CypherDropPlacement extends CypherAdminCommand implements ExecutableStatement {

    private final String storeName;
    private final String databaseName;


    public CypherDropPlacement(
            ParserPos pos,
            CypherSimpleEither<String, CypherParameter> databaseName,
            CypherSimpleEither<String, CypherParameter> storeName ) {
        super( pos );
        this.databaseName = getNameOrNull( databaseName );
        this.storeName = getNameOrNull( storeName );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        AdapterManager adapterManager = AdapterManager.getInstance();

        List<LogicalNamespace> graphs = statement.getTransaction().getSnapshot().getNamespaces( new Pattern( this.databaseName ) );

        DataStore<?> dataStore = Stream.of( storeName )
                .map( store -> adapterManager.getStore( storeName ).orElseThrow() )
                .toList().get( 0 );

        if ( graphs.size() != 1 ) {
            throw new GenericRuntimeException( "Error while adding graph placement" );
        }

        DdlManager.getInstance().dropGraphPlacement( graphs.get( 0 ).id, dataStore, statement );
    }

}
