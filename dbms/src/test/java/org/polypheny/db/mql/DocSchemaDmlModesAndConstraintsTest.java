/*
 * Copyright 2019-2026 The Polypheny Project
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.bson.BsonArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;

/**
 * Additional DML coverage for document schemas.
 *
 * Focus:
 *  - validationAction WARN / OFF on writes
 *  - extended scalar/object/array constraints
 *    (pattern, minLength/maxLength, enum, const, multipleOf, minItems/maxItems, min/maxProperties)
 */
@Tag("adapter")
public class DocSchemaDmlModesAndConstraintsTest extends MqlTestTemplate {

    private static final String USER = "user_dml_modes";

    private void dropUserCollectionIfExists() {
        try {
            dropCollection( USER );
        } catch ( Exception ignored ) {
            // ignore
        }
    }


    private void recreateUserCollection( String optionsObjectLiteral ) {
        dropUserCollectionIfExists();
        assertDoesNotThrow( () -> execute( "db.createCollection(\"" + USER + "\", " + optionsObjectLiteral + ")" ) );
    }


    private void createUserCollectionWithSimpleSchema( String validationAction ) {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }" +
                "    }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"" + validationAction + "\"" +
                "}" );
    }


    private void createUserCollectionWithExtendedConstraintSchema( String validationAction ) {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      code: { type: \"text\", minLength: 3, maxLength: 5, pattern: \"^[a-z]+$\" }," +
                "      role: { type: \"text\", enum: [\"admin\",\"user\"] }," +
                "      status: { type: \"text\", const: \"active\" }," +
                "      amount: { type: \"number\", multipleOf: 0.5 }," +
                "      tags: { type: \"array\", items: { type: \"text\" }, minItems: 1, maxItems: 2 }," +
                "      metadata: { type: \"object\", properties: {}, required: [], additionalProperties: true, minProperties: 1, maxProperties: 2 }" +
                "    }," +
                "    required: [\"code\",\"role\",\"status\",\"amount\",\"tags\",\"metadata\"]," +
                "    additionalProperties: false" +
                "  }," +
                "  validationAction: \"" + validationAction + "\"" +
                "}" );
    }


    private void assertDocs( List<String> expectedDocs ) {
        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, expectedDocs, true, true ) );
    }


    private void assertProjectionMatches( String projection, String expectedArrayJson ) {
        String normalizedProjection = projection
                .replace( "\"_id\":0,", "" )
                .replace( ",\"_id\":0", "" )
                .replace( "\"_id\":0", "" )
                .replace( "{,", "{" )
                .replace( ",}", "}" );

        if ( normalizedProjection.isBlank() || normalizedProjection.equals( "{" ) ) {
            normalizedProjection = "{}";
        }

        DocResult result = find( "{}", normalizedProjection, USER );
        List<String> expectedDocs = BsonArray.parse( expectedArrayJson )
                .getValues()
                .stream()
                .map( Object::toString )
                .toList();
        assertTrue( MongoConnection.checkDocResultSet( result, expectedDocs, true, true ) );
    }


    @AfterEach
    public void teardownUser() {
        try {
            deleteMany( "{}", USER );
        } catch ( Exception ignored ) {
            // ignore
        }
        dropUserCollectionIfExists();
    }


    @Test
    public void insert_wrongTypeUnderWarn_shouldSucceed_andPersistDocument() {
        createUserCollectionWithSimpleSchema( "warn" );

        String invalidButAllowed = "{\"name\":123}";
        assertDoesNotThrow( () -> insert( invalidButAllowed, USER ) );

        assertDocs( ImmutableList.of( invalidButAllowed ) );
    }


    @Test
    public void update_wrongTypeUnderWarn_shouldSucceed_andPersistChange() {
        createUserCollectionWithSimpleSchema( "warn" );
        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        assertDoesNotThrow( () -> update( "{\"name\":\"Alice\"}", "{\"$set\":{\"name\":123}}", USER ) );

        assertProjectionMatches( "{\"_id\":0,\"name\":1}", "[{\"name\":123}]" );
    }


    @Test
    public void insert_wrongTypeUnderOff_shouldSucceed_andPersistDocument() {
        createUserCollectionWithSimpleSchema( "off" );

        String invalidButAllowed = "{\"name\":123}";
        assertDoesNotThrow( () -> insert( invalidButAllowed, USER ) );

        assertDocs( ImmutableList.of( invalidButAllowed ) );
    }


    @Test
    public void update_wrongTypeUnderOff_shouldSucceed_andPersistChange() {
        createUserCollectionWithSimpleSchema( "off" );
        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        assertDoesNotThrow( () -> update( "{\"name\":\"Alice\"}", "{\"$set\":{\"name\":123}}", USER ) );

        assertProjectionMatches( "{\"_id\":0,\"name\":1}", "[{\"name\":123}]" );
    }


    @Test
    public void insert_extendedConstraints_validDocument_shouldSucceed() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        String valid = "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\",\"y\"],\"metadata\":{\"a\":1}}";
        assertDoesNotThrow( () -> insert( valid, USER ) );

        assertDocs( ImmutableList.of( valid ) );
    }


    @Test
    public void insert_patternViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"ab1\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_minLengthViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"ab\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_maxLengthViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcdef\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_enumViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"guest\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_constViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"inactive\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_multipleOfViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.3,\"tags\":[\"x\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_minItemsViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_maxItemsViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\",\"y\",\"z\"],\"metadata\":{\"a\":1}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_minPropertiesViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }


    @Test
    public void insert_maxPropertiesViolation_shouldFail_andInsertNothing() {
        createUserCollectionWithExtendedConstraintSchema( "strict" );

        assertThrows( Exception.class, () -> insert(
                "{\"code\":\"abcd\",\"role\":\"admin\",\"status\":\"active\",\"amount\":1.5,\"tags\":[\"x\"],\"metadata\":{\"a\":1,\"b\":2,\"c\":3}}",
                USER ) );

        assertDocs( ImmutableList.of() );
    }

}
