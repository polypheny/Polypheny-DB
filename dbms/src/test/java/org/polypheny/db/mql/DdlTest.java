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

package org.polypheny.db.mql;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogNamespace;
import org.polypheny.db.catalog.exceptions.UnknownNamespaceException;

@SuppressWarnings("SqlNoDataSourceInspection")
public class DdlTest extends MqlTestTemplate {

    final static String collectionName = "doc";


    @Test
    public void addCollectionTest() throws UnknownNamespaceException {
        Catalog catalog = Catalog.getInstance();

        execute( "db.createCollection(\"" + collectionName + "\")" );

        CatalogNamespace namespace = catalog.getNamespace( Catalog.defaultDatabaseId, database );

        assertEquals( 1, catalog.getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", collectionName ) );

        assertEquals( 0, catalog.getCollections( namespace.id, null ).size() );

        execute( "db.createCollection(\"" + collectionName + "\")" );

        assertEquals( 1, catalog.getCollections( namespace.id, null ).size() );
    }


    @Test
    public void addPlacementTest() throws UnknownNamespaceException, SQLException {
        Catalog catalog = Catalog.getInstance();

        execute( "db.createCollection(\"" + collectionName + "\")" );

        CatalogNamespace namespace = catalog.getNamespace( Catalog.defaultDatabaseId, database );

        CatalogCollection collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

        assertEquals( collection.placements.size(), 1 );

        addStore( "store1" );

        execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, "store1" ) );

        collection = catalog.getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

        assertEquals( collection.placements.size(), 2 );

    }


    private void addStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "ALTER ADAPTERS ADD \"" + name + "\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

            }
        }
    }

}
