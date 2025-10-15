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

package org.polypheny.db.mql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.webui.models.results.DocResult;

@SuppressWarnings("SqlNoDataSourceInspection")
@Tag("adapter")
public class DdlTest extends MqlTestTemplate {

    final static String collectionName = "doc";


    @Test
    public void addCollectionTest() {
        String name = "testCollection";

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( MqlTestTemplate.namespace ).orElseThrow();

        int size = Catalog.snapshot().doc().getCollections( namespace.id, null ).size();

        execute( "db.createCollection(\"" + name + "\")" );

        assertEquals( size + 1, Catalog.snapshot().doc().getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", name ) );

        assertEquals( size, Catalog.snapshot().doc().getCollections( namespace.id, null ).size() );

        execute( "db.createCollection(\"" + name + "\")" );

        assertEquals( size + 1, Catalog.snapshot().doc().getCollections( namespace.id, null ).size() );

        execute( String.format( "db.%s.drop()", name ) );
    }


    @Test
    public void differentNamespaceSyntaxTest() {
        String name = "testNamespaceSyntax";

        execute( namespace + ".createCollection(\"" + name + "\")" );

        execute( "db." + name + ".find({})" );

        execute( name + ".find({})" );

        execute( namespace + "." + name + ".find({})" );

        execute( String.format( "%s.%s.drop()", namespace, name ) );

    }


    @Test
    public void addPlacementTest() throws SQLException {

        String placement = "store1";
        try {
            LogicalNamespace namespace = Catalog.snapshot().getNamespace( MqlTestTemplate.namespace ).orElseThrow();

            List<String> collectionNames = Catalog.snapshot().doc().getCollections( namespace.id, null ).stream().map( c -> c.name ).toList();
            collectionNames.forEach( n -> execute( String.format( "db.%s.drop()", n ) ) );

            execute( "db.createCollection(\"" + collectionName + "\")" );

            LogicalCollection collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 1 );

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 2 );

        } finally {
            execute( String.format( "db.%s.drop()", collectionName ) );
            removeStore( placement );
        }

    }


    @Test
    public void deletePlacementTest() throws SQLException {

        String placement = "store1";

        execute( "db.createCollection(\"" + collectionName + "\")" );

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( MqlTestTemplate.namespace ).orElseThrow();

        LogicalCollection collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

        assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 1 );

        addStore( placement );

        try {
            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 2 );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, placement ) );

            collection = Catalog.snapshot().doc().getCollections( namespace.id, new Pattern( collectionName ) ).get( 0 );

            assertEquals( Catalog.snapshot().alloc().getFromLogical( collection.id ).size(), 1 );

            execute( String.format( "db.%s.drop()", collectionName ) );
        } catch ( Exception e ) {
            execute( String.format( "db.%s.drop()", collectionName ) );
        } finally {
            removeStore( placement );
        }
    }


    @Test
    public void deletePlacementDataTest() throws SQLException {

        String placement = "store1";
        final String DATA = "{ \"key\": \"value\", \"key1\": \"value1\"}";

        execute( "db.createCollection(\"" + collectionName + "\")" );

        insert( DATA );

        try {

            addStore( placement );

            execute( String.format( "db.%s.addPlacement(\"%s\")", collectionName, placement ) );

            execute( String.format( "db.%s.deletePlacement(\"%s\")", collectionName, "hsqldb" ) );

            DocResult result = find( "{}", "{}" );

            assertTrue(
                    MongoConnection.checkDocResultSet(
                            result,
                            ImmutableList.of( DATA ), true,
                            false ) );


        } finally {
            execute( String.format( "db.%s.drop()", collectionName ) );

            removeStore( placement );
        }
    }


    private void addStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "ALTER ADAPTERS ADD \"" + name + "\" USING 'Hsqldb' AS 'Store'"
                        + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

            }
        }
    }


    private void removeStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "ALTER ADAPTERS DROP \"" + name + "\"" );

            }
        }
    }




    // --------------------------------- NEW TESTS --------------------------------- //

    // ADD THESE TESTS inside class DdlTest (do not modify existing ones)

    @Test
    public void createCollection_withSchema_strict_validAndInvalidInserts() {
        final String name = "users_strict_noextras";
        final String opts = usersOptions(/*allowExtras=*/false, /*enforcement=*/"strict");

        // ensure clean slate
        dropSilently(name);

        try {
            // create with schema + STRICT + additionalProperties=false
            execute("db.createCollection(\"" + name + "\"," + opts + ")");

            // valid doc -> OK
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Ada\", lastname: \"Lovelace\" }, age: 36 })"));

            // extra top-level field -> REJECT (additionalProperties=false)
            assertThrows(Exception.class, () -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Alan\", lastname: \"Turing\" }, age: 41, nickname: \"Al\" })"));

            // missing required field 'age' -> REJECT
            assertThrows(Exception.class, () -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Grace\", lastname: \"Hopper\" } })"));

            // type mismatch: age as string -> REJECT
            assertThrows(Exception.class, () -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Tim\", lastname: \"Berners-Lee\" }, age: \"35\" })"));

        } finally {
            dropSilently(name);
        }
    }


    @Test
    public void createCollection_withSchema_strict_allowExtras() {
        final String name = "users_strict_extras";
        final String opts = usersOptions(/*allowExtras=*/true, /*enforcement=*/"strict");

        dropSilently(name);

        try {
            execute("db.createCollection(\"" + name + "\"," + opts + ")");

            // valid with extra field -> OK (extras allowed)
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Ada\", lastname: \"Lovelace\" }, age: 36, nickname: \"Ada\" })"));

            // missing required field still not allowed under STRICT
            assertThrows(Exception.class, () -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Alan\", lastname: \"Turing\" } })"));

            // type mismatch still not allowed under STRICT
            assertThrows(Exception.class, () -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Grace\", lastname: \"Hopper\" }, age: \"37\" })"));

        } finally {
            dropSilently(name);
        }
    }


    @Test
    public void createCollection_withSchema_warn_allowsInvalidDocuments() {
        final String name = "users_warn";
        final String opts = usersOptions(/*allowExtras=*/false, /*enforcement=*/"warn");

        dropSilently(name);

        try {
            execute("db.createCollection(\"" + name + "\"," + opts + ")");

            // Valid -> OK
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Ada\", lastname: \"Lovelace\" }, age: 36 })"));

            // Extra field -> should still be accepted in WARN
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Alan\", lastname: \"Turing\" }, age: 41, nickname: \"Al\" })"));

            // Missing required field -> should still be accepted in WARN
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Grace\", lastname: \"Hopper\" } })"));

            // Type mismatch -> should still be accepted in WARN
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ name: { firstname: \"Tim\", lastname: \"Berners-Lee\" }, age: \"35\" })"));

        } finally {
            dropSilently(name);
        }
    }


    @Test
    public void createCollection_withSchema_off_allowsAnything() {
        final String name = "users_off";
        // enforcement OFF; extras flag shouldn't matter, but keep it explicit
        final String opts = usersOptions(/*allowExtras=*/false, /*enforcement=*/"off");

        dropSilently(name);

        try {
            execute("db.createCollection(\"" + name + "\"," + opts + ")");

            // Everything should be accepted with OFF
            assertDoesNotThrow(() -> execute(
                    "db." + name + ".insert({ any: { crazy: [1,2,3] }, shape: \"whatever\", x: null })"));

        } finally {
            dropSilently(name);
        }
    }


    @Test
    public void createCollection_rejects_legacyValidatorKey() {
        final String name = "users_validator_legacy";

        dropSilently(name);

        try {
            // Using legacy 'validator.$jsonSchema' must be rejected by resolver
            String badOpts =
                    "{ validator: { $jsonSchema: { properties: { name: { type: \"object\" } } } }, validationAction: \"strict\" }";

            assertThrows(Exception.class, () -> execute(
                    "db.createCollection(\"" + name + "\"," + badOpts + ")"));

        } finally {
            dropSilently(name);
        }
    }


    @Test
    public void createCollection_rejects_requiredKeyword() {
        final String name = "users_required_keyword";

        dropSilently(name);

        try {
            String badOpts =
                    "{ docSchema: { type: \"object\", properties: { name: { type: \"object\", properties: { firstname: { type: \"text\" } } }, age: { type: \"number\" } }, required: [\"name\",\"age\"], additionalProperties: false }, validationAction: \"strict\" }";

            assertThrows(Exception.class, () -> execute(
                    "db.createCollection(\"" + name + "\"," + badOpts + ")"));

        } finally {
            dropSilently(name);
        }
    }


    @Test
    public void createCollection_rejects_nonObjectDocSchema() {
        final String name = "users_bad_docSchema";

        dropSilently(name);

        try {
            String badOpts = "{ docSchema: \"text\", validationAction: \"strict\" }";

            assertThrows(Exception.class, () -> execute(
                    "db.createCollection(\"" + name + "\"," + badOpts + ")"));

        } finally {
            dropSilently(name);
        }
    }


// -------------------- helpers (private) --------------------

    /**
     * Produces the options payload for users schema in your dialect.
     * All declared fields are required by design; 'additionalProperties' toggles extras.
     */
    private static String usersOptions(boolean allowExtras, String enforcement) {
        // additionalProperties: false | true
        String ap = allowExtras ? "true" : "false";
        return "{ " +
                "docSchema: {" +
                "  type: \"object\"," +
                "  properties: {" +
                "    name: {" +
                "      type: \"object\"," +
                "      properties: {" +
                "        firstname: { type: \"text\" }," +
                "        lastname:  { type: \"text\" }" +
                "      }" +
                "    }," +
                "    age: { type: \"number\" }" +
                "  }," +
                "  additionalProperties: " + ap +
                "}," +
                "validationAction: \"" + enforcement + "\"" +
                "}";
    }




}
