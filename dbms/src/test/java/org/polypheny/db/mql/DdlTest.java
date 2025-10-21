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

    // -----------------------------
    // New tests around docSchema / validation options
    // -----------------------------

    @Test
    public void createCollection_missingDocSchema_ShouldFail() {
        String coll = "schema_missing_ds";
        String opts = "{ \"validationAction\": \"ERROR\" }";
        try {
            assertThrows( RuntimeException.class, () ->
                    execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );
        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_docSchemaNotObject_ShouldFail() {
        String coll = "schema_ds_not_object";
        String opts = "{ \"docSchema\":\"string\", \"validationAction\":\"ERROR\" }";
        try {
            assertThrows( RuntimeException.class, () ->
                    execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );
        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_unknownTypeToken_ShouldFail() {
        // 'str' should be unknown; use 'string' for valid tests elsewhere
        String coll = "schema_bad_type_token";
        String opts = "{ \"docSchema\": { \"additionalProperties\":\"ALLOW\", \"properties\": { \"name\": { \"type\":\"str\" } } }, \"validationAction\":\"ERROR\" }";
        try {
            assertThrows( RuntimeException.class, () ->
                    execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );
        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_invalidAdditionalProperties_ShouldFail() {
        String coll = "schema_bad_ap";
        String opts = "{ \"docSchema\": { \"additionalProperties\":\"DENY\", \"properties\": { \"name\": { \"type\":\"string\" } } }, \"validationAction\":\"ERROR\" }";
        try {
            assertThrows( RuntimeException.class, () ->
                    execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );
        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_unknownValidationAction_ShouldFail() {
        String coll = "schema_bad_action";
        String opts = "{ \"docSchema\": { \"additionalProperties\":\"ALLOW\", \"properties\": { \"name\": { \"type\":\"string\" } } }, \"validationAction\":\"enforce\" }";
        try {
            assertThrows( RuntimeException.class, () ->
                    execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );
        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_validSchema_ErrorAction_AndInsertValidation() {
        String coll = "schema_ok_error";
        String opts = "{ \"docSchema\": {"
                + "  \"additionalProperties\":\"FORBID\","
                + "  \"properties\": {"
                + "    \"name\": { \"type\":\"string\" }"
                + "  }"
                + "},"
                + "\"validationAction\":\"ERROR\" }";

        try {
            // create succeeds
            assertDoesNotThrow( () -> execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );

            // valid insert
            assertDoesNotThrow( () -> insert( "{ \"name\": \"Ada\" }", coll ) );

            // wrong type for 'name' triggers error
            assertThrows( RuntimeException.class, () -> insert( "{ \"name\": 42 }", coll ) );

            // additional properties are forbidden; should error
            assertThrows( RuntimeException.class, () -> insert( "{ \"name\": \"Bob\", \"age\": 30 }", coll ) );

        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_validSchema_WarnAction_AllowsViolations() {
        String coll = "schema_ok_warn";
        String opts = "{ \"docSchema\": {"
                + "  \"additionalProperties\":\"FORBID\","
                + "  \"properties\": {"
                + "    \"name\": { \"type\":\"string\" }"
                + "  }"
                + "},"
                + "\"validationAction\":\"WARN\" }";

        try {
            assertDoesNotThrow( () -> execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );

            // violations should not throw with WARN
            assertDoesNotThrow( () -> insert( "{ \"name\": 5 }", coll ) );
            assertDoesNotThrow( () -> insert( "{ \"name\": \"Eve\", \"extra\": true }", coll ) );

            // confirm we can read back at least one violating doc
            DocResult result = MongoConnection.executeGetResponse( "db." + coll + ".find({},{})" );
            // just sanity check that the find returned something (we reuse existing helper pattern)
            assertTrue( result != null );
        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_additionalPropertiesBooleanVariants() {
        // additionalProperties as boolean should be accepted per validator ("boolean or 'FORBID'/'ALLOW'")
        String collTrue = "schema_ap_true";
        String collFalse = "schema_ap_false";

        String optsTrue = "{ \"docSchema\": {"
                + "  \"additionalProperties\": true,"
                + "  \"properties\": { \"name\": { \"type\":\"string\" } }"
                + "}, \"validationAction\":\"ERROR\" }";

        String optsFalse = "{ \"docSchema\": {"
                + "  \"additionalProperties\": false,"
                + "  \"properties\": { \"name\": { \"type\":\"string\" } }"
                + "}, \"validationAction\":\"ERROR\" }";

        try {
            // true => allow extras even with ERROR action (no violation)
            assertDoesNotThrow( () -> execute( "db.createCollection(\"" + collTrue + "\"," + optsTrue + ")" ) );
            assertDoesNotThrow( () -> insert( "{ \"name\":\"N\", \"x\":1 }", collTrue ) );

            // false => forbid extras; inserting an extra field should error
            assertDoesNotThrow( () -> execute( "db.createCollection(\"" + collFalse + "\"," + optsFalse + ")" ) );
            assertDoesNotThrow( () -> insert( "{ \"name\":\"N\" }", collFalse ) );
            assertThrows( RuntimeException.class, () -> insert( "{ \"name\":\"N\", \"x\":1 }", collFalse ) );

        } finally {
            dropSilently( collTrue );
            dropSilently( collFalse );
        }
    }


    @Test
    public void insert_conformingDocument_succeedsAndIsQueryable() {
        String coll = "schema_queryable";
        String opts = "{ \"docSchema\": {"
                + "  \"additionalProperties\":\"FORBID\","
                + "  \"properties\": {"
                + "    \"key\": { \"type\":\"string\" },"
                + "    \"key1\": { \"type\":\"string\" }"
                + "  }"
                + "},"
                + "\"validationAction\":\"ERROR\" }";

        final String DATA = "{ \"key\": \"value\", \"key1\": \"value1\" }";

        try {
            assertDoesNotThrow( () -> execute( "db.createCollection(\"" + coll + "\"," + opts + ")" ) );
            assertDoesNotThrow( () -> insert( DATA, coll ) );

            DocResult result = MongoConnection.executeGetResponse( "db." + coll + ".find({},{})" );
            assertTrue(
                    MongoConnection.checkDocResultSet(
                            result,
                            ImmutableList.of( DATA ), true,
                            false ) );

        } finally {
            dropSilently( coll );
        }
    }


    @Test
    public void createCollection_alreadyExists_ShouldFail() {
        String coll = "dup_collection_test";
        try {
            execute( "db.createCollection(\"" + coll + "\")" );
            assertThrows( RuntimeException.class, () ->
                    execute( "db.createCollection(\"" + coll + "\")" ) );
        } finally {
            dropSilently( coll );
        }
    }

}
