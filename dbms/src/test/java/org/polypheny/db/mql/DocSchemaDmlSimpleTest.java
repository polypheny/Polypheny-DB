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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;

/**
 * DML tests for document schemas.
 *
 * Focus:
 *  - required (explicit + default-all)
 *  - root + nested additionalProperties
 *  - scalar union types ("OR" via type array)
 *  - composition (oneOf/anyOf/allOf/not)
 *  - array constraints (uniqueItems)
 */
@Tag("adapter")
public class DocSchemaDmlSimpleTest extends MqlTestTemplate {

    private static final String USER = "user";


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


    /**
     * Base schema used for most tests:
     *  - required: ["name"] (age/nickname/tags optional)
     *  - root additionalProperties: true
     *  - nickname allows OR: text|null
     *  - tags uniqueItems
     */
    private void createUserCollectionWithSimpleSchema() {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      age: { type: \"number\" }," +
                "      nickname: { type: [\"text\",\"null\"] }," +
                "      tags: { type: \"array\", items: { type: \"text\" }, uniqueItems: true }" +
                "    }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );
    }


    @BeforeEach
    public void setupUser() {
        createUserCollectionWithSimpleSchema();
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


    /**
     * name: "Alice"
     */
    @Test
    public void insert_validDocument_shouldSucceed() {
        String alice = "{\"name\":\"Alice\"}";

        assertDoesNotThrow( () -> insert( alice, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( alice ),
                        true,
                        true
                )
        );
    }


    /**
     * name: "Bob", extra: 42
     */
    @Test
    public void insert_withAdditionalField_shouldSucceed() {
        // additionalProperties: true -> extra fields are allowed (even if not declared in properties)
        String doc = "{\"name\":\"Bob\",\"extra\":42}";

        assertDoesNotThrow( () -> insert( doc, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( doc ),
                        true,
                        true
                )
        );
    }


    /**
     * name: "Bob", age: 42, nickname: null, tags: ["a","b"]
     */
    @Test
    public void insert_withOptionalFieldsAndOrType_shouldSucceed() {
        String doc = "{\"name\":\"Bob\",\"age\":42,\"nickname\":null,\"tags\":[\"a\",\"b\"]}";

        assertDoesNotThrow( () -> insert( doc, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( doc ),
                        true,
                        true
                )
        );
    }


    /**
     * nickname: 123 (invalid: nickname is text|null)
     */
    @Test
    public void insert_orTypeWrongScalar_shouldFail_andInsertNothing() {
        String invalid = "{\"name\":\"Bob\",\"nickname\":123}";

        assertThrows( Exception.class, () -> insert( invalid, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(),
                        true,
                        true
                )
        );
    }


    /**
     * tags must have unique items
     */
    @Test
    public void insert_uniqueItemsViolation_shouldFail_andInsertNothing() {
        String invalid = "{\"name\":\"Bob\",\"tags\":[\"a\",\"a\"]}";

        assertThrows( Exception.class, () -> insert( invalid, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), true, true ) );
    }


    /**
     * age: 42
     */
    @Test
    public void insert_missingRequiredField_shouldFail_andInsertNothing() {
        // required: ["name"] -> "name" must exist
        String invalid = "{\"age\":42}";

        assertThrows( Exception.class, () -> insert( invalid, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(),
                        true,
                        true
                )
        );
    }


    /**
     * name: null
     */
    @Test
    public void insert_nullRequiredField_shouldFail_andInsertNothing() {
        String invalid = "{\"name\":null}";

        assertThrows( Exception.class, () -> insert( invalid, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), true, true ) );
    }


    /**
     * name: 123
     */
    @Test
    public void insert_wrongTypeForName_shouldFail_andInsertNothing() {
        String invalid = "{\"name\":123}";

        assertThrows( Exception.class, () -> insert( invalid, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), true, true ) );
    }


    /**
     * name: "Alice"
     * name: "Bob"
     */
    @Test
    public void insertMany_shouldSucceed_andInsertDocuments() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}"
        );

        assertDoesNotThrow( () -> insertMany( data, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"name\":\"Alice\"}",
                                "{\"name\":\"Bob\"}"
                        ),
                        true,
                        true
                )
        );
    }


    /**
     * name: "Alice"
     * name: 123
     */
    @Test
    public void insertMany_withOneInvalidDocument_shouldFail_andInsertNothing() {
        // should fail the whole statement before execution (no partial insert)
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":123}" // invalid
        );

        assertThrows(
                Exception.class,
                () -> execute( "db.user.insertMany([" + String.join( ",", data ) + "])" )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), true, true ) );
    }


    // -----------------------
    // UPDATE tests (schema)
    // -----------------------

    @Test
    public void update_setValid_shouldSucceed() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"name\":\"Dave\"}}", USER );

        DocResult result = find( "{}", "{}", USER );

        List<String> updated = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Dave\"}"
        );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        updated,
                        true,
                        true ) );
    }


    @Test
    public void update_setWrongType_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );

        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"name\":5}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );

        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * $set name to null must fail (name is required + TEXT).
     */
    @Test
    public void update_setNull_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"name\":null}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * $set name to an object must fail (expects TEXT).
     */
    @Test
    public void update_setObject_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"name\":{\"first\":\"Dave\"}}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * $set name to an array must fail (expects TEXT).
     */
    @Test
    public void update_setArray_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"name\":[\"Dave\"]}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * $set an additional (unknown) field should succeed because additionalProperties=true.
     */
    @Test
    public void update_setAdditionalField_shouldSucceed() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertDoesNotThrow(
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"extra\":42}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );

        List<String> expected = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\",\"extra\":42}"
        );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    /**
     * $set an additional object field should succeed because additionalProperties=true.
     */
    @Test
    public void update_setAdditionalFieldToDocument_shouldSucceed() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertDoesNotThrow(
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"profile\":{\"x\":1}}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );

        List<String> expected = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\",\"profile\":{\"x\":1}}"
        );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    /**
     * insertMany with nested object (strings only) should succeed.
     */
    @Test
    public void insertMany_withNestedObjectWithStrings_shouldSucceed() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        assertDoesNotThrow( () -> insertMany( data, USER ) );

        assertDoesNotThrow(
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"profile\":{\"first\":\"foo\",\"last\":\"bar\"}}}", USER )
        );

        List<String> expected = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\",\"profile\":{\"first\":\"foo\",\"last\":\"bar\"}}"
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    /**
     * If one field in a multi-field $set is invalid, the whole update must fail and apply nothing.
     */
    @Test
    public void update_setMixedValidAndInvalid_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$set\":{\"age\":42,\"name\":5}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * $unset required field "name" should fail and keep original.
     */
    @Test
    public void update_unsetRequiredField_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$unset\":{\"name\":\"\"}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * $unset optional field "age" should succeed.
     */
    @Test
    public void update_unsetOptionalField_shouldSucceed() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\",\"age\":42}"
        );
        insertMany( data, USER );

        assertDoesNotThrow(
                () -> update( "{\"name\":\"Charlie\"}", "{\"$unset\":{\"age\":0}}", USER )
        );

        List<String> expected = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    /**
     * $rename required field "name" should fail (would remove required property).
     */
    @Test
    public void update_renameRequiredField_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$rename\":{\"name\":\"fullName\"}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * Numeric operators on TEXT field should fail.
     */
    @Test
    public void update_incOnTextField_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"$inc\":{\"name\":1}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * Replacement update (no $operator) with valid document should succeed.
     */
    @Test
    public void update_replaceWithValidDocument_shouldSucceed() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertDoesNotThrow(
                () -> update( "{\"name\":\"Charlie\"}", "{\"name\":\"Dave\"}", USER )
        );

        DocResult result = find( "{}", "{}", USER );

        List<String> expected = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Dave\"}"
        );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    /**
     * Replacement update that removes required field should fail and keep original.
     */
    @Test
    public void update_replaceWithMissingRequiredField_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Charlie\"}", "{\"age\":42}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * Valid update with a filter that matches nothing should not throw and should not change data.
     */
    @Test
    public void update_noMatchWithValidUpdate_shouldSucceed_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertDoesNotThrow(
                () -> update( "{\"name\":\"Nobody\"}", "{\"$set\":{\"name\":\"X\"}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    /**
     * Invalid update should fail even if the filter matches nothing (validated before execution).
     */
    @Test
    public void update_noMatchWithInvalidUpdate_shouldFail_andKeepOriginal() {
        List<String> data = List.of(
                "{\"name\":\"Alice\"}",
                "{\"name\":\"Bob\"}",
                "{\"name\":\"Charlie\"}"
        );
        insertMany( data, USER );

        assertThrows(
                Exception.class,
                () -> update( "{\"name\":\"Nobody\"}", "{\"$set\":{\"name\":5}}", USER )
        );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, data, true, true ) );
    }


    // -------------------------------------------------------------------------
    // New coverage: nested additionalProperties, default-required, oneOf/anyOf/allOf/not
    // -------------------------------------------------------------------------

    @Test
    public void insert_nestedAdditionalProperties_forbid_shouldRejectExtrasInSubdocument() {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      profile: {" +
                "        type: \"object\"," +
                "        properties: { first: { type: \"text\" } }," +
                "        required: [\"first\"]," +
                "        additionalProperties: false" +
                "      }" +
                "    }," +
                "    required: [\"name\",\"profile\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        String ok = "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\"}}";
        assertDoesNotThrow( () -> insert( ok, USER ) );

        String bad = "{\"name\":\"Bob\",\"profile\":{\"first\":\"B\",\"x\":1}}";
        assertThrows( Exception.class, () -> insert( bad, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of( ok ), true, true ) );
    }


    @Test
    public void insert_rootAdditionalProperties_forbid_butNestedAllow_shouldAcceptNestedExtrasOnly() {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      profile: {" +
                "        type: \"object\"," +
                "        properties: { first: { type: \"text\" } }," +
                "        required: [\"first\"]," +
                "        additionalProperties: true" +
                "      }" +
                "    }," +
                "    required: [\"name\",\"profile\"]," +
                "    additionalProperties: false" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        // root extras forbidden
        String badRoot = "{\"name\":\"Alice\",\"x\":1,\"profile\":{\"first\":\"A\"}}";
        assertThrows( Exception.class, () -> insert( badRoot, USER ) );

        // but nested extras allowed
        String ok = "{\"name\":\"Bob\",\"profile\":{\"first\":\"B\",\"x\":1}}";
        assertDoesNotThrow( () -> insert( ok, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of( ok ), true, true ) );
    }


    @Test
    public void insert_defaultRequired_allDeclaredPropertiesRequired_whenRequiredOmitted() {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      age:  { type: \"number\" }" +
                "    }," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        // required omitted -> dialect default = all declared properties required (name + age)
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Alice\"}", USER ) );
        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"age\":1}", USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of( "{\"name\":\"Bob\",\"age\":1}" ), true, true ) );
    }


    @Test
    public void insert_oneOf_shouldAcceptExactlyOneBranch() {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      contact: {" +
                "        oneOf: [" +
                "          { type: \"object\", properties: { email: { type: \"text\" } }, required: [\"email\"], additionalProperties: true }," +
                "          { type: \"object\", properties: { phone: { type: \"text\" } }, required: [\"phone\"], additionalProperties: true }" +
                "        ]" +
                "      }" +
                "    }," +
                "    required: [\"name\",\"contact\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        String email = "{\"name\":\"Alice\",\"contact\":{\"email\":\"a@b.com\"}}";
        String phone = "{\"name\":\"Bob\",\"contact\":{\"phone\":\"123\"}}";
        String both = "{\"name\":\"Charlie\",\"contact\":{\"email\":\"c@d.com\",\"phone\":\"456\"}}";

        assertDoesNotThrow( () -> insert( email, USER ) );
        assertDoesNotThrow( () -> insert( phone, USER ) );
        assertThrows( Exception.class, () -> insert( both, USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of( email, phone ), true, true ) );
    }


    @Test
    public void insert_anyOf_allOf_not_shouldValidate() {
        recreateUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      status: { anyOf: [ { type: \"text\", const: \"active\" }, { type: \"text\", const: \"pending\" } ] }," +
                "      score:  { allOf: [ { type: \"number\", minimum: 0 }, { type: \"number\", maximum: 10 } ] }," +
                "      token:  { not: { type: \"null\" } }" +
                "    }," +
                "    required: [\"name\",\"status\",\"score\",\"token\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        String ok = "{\"name\":\"Alice\",\"status\":\"active\",\"score\":5,\"token\":\"t\"}";
        assertDoesNotThrow( () -> insert( ok, USER ) );

        assertThrows( Exception.class, () -> insert( "{\"name\":\"Bob\",\"status\":\"inactive\",\"score\":5,\"token\":\"t\"}", USER ) );
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Carl\",\"status\":\"active\",\"score\":20,\"token\":\"t\"}", USER ) );
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Dana\",\"status\":\"active\",\"score\":5,\"token\":null}", USER ) );

        DocResult result = find( "{}", "{}", USER );
        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of( ok ), true, true ) );
    }

}
