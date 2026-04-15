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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ALTER COLLECTION SCHEMA tests.
 *
 * Focus: strict-mode denial + preflight behavior.
 */
@Tag("adapter")
public class DocSchemaAlterCollectionSchemaTest extends MqlTestTemplate {

    private static final String USER = "user";

    @BeforeAll
    public static void useOwnNamespace() {
        namespace = "test_docschema_alter";
        initDatabase();
    }

    @AfterAll
    public static void cleanupOwnNamespace() {
        dropDatabase();
        namespace = "test";
    }


    private void dropUserCollectionIfExists() {
        try {
            dropCollection( USER );
        } catch ( Exception ignored ) {
            // ignore
        }
    }


    private void createUserCollection( String optionsObjectLiteral ) {
        dropUserCollectionIfExists();
        assertDoesNotThrow( () -> execute( "db.createCollection(\"" + USER + "\", " + optionsObjectLiteral + ")" ) );
    }


    private void createUserCollectionWithoutSchema() {
        dropUserCollectionIfExists();
        assertDoesNotThrow( () -> execute( "db.createCollection(\"" + USER + "\")" ) );
    }


    private void alterUserSchema( String optionsObjectLiteral ) {
        execute( "db.alterCollectionSchema(\"" + USER + "\", " + optionsObjectLiteral + ")" );
    }


    @AfterEach
    public void cleanupUser() {
        dropUserCollectionIfExists();
    }


    @Test
    public void alterSchema_strict_forbidExtras_withExistingExtras_shouldFail_andNotChangeSchema() {
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        // valid under AP=true
        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"age\":1}", USER ) );

        // tighten AP to FORBID under STRICT -> must preflight & deny
        assertThrows( Exception.class, () -> alterUserSchema( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: false" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" ) );

        // old schema must still be active (extra fields still allowed)
        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"age\":2}", USER ) );
    }


    @Test
    public void alterSchema_warn_forbidExtras_withExistingExtras_shouldSucceed_butTightenToStrictShouldFail() {
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"age\":1}", USER ) );

        // same tightening, but final enforcement = WARN -> allowed even with violations
        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: false" +
                "  }," +
                "  validationAction: \"warn\"" +
                "}" ) );

        // If the new schema was persisted, tightening enforcement-only to STRICT must now fail.
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_strict_addRequiredField_withExistingDocsMissingField_shouldFail_andNotChangeSchema() {
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // Make 'age' required via explicit required list -> must preflight & deny under STRICT.
        assertThrows( Exception.class, () -> alterUserSchema( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      age:  { type: \"number\" }" +
                "    }," +
                "    required: [\"name\",\"age\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" ) );

        // old schema must still be active: inserting without 'age' should still work
        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\"}", USER ) );
    }


    @Test
    public void alterSchema_warn_addRequiredField_shouldSucceed_butTightenToStrictShouldFail() {
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // Apply schema even though it would invalidate existing docs (WARN tolerates legacy violations)
        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      age:  { type: \"number\" }" +
                "    }," +
                "    required: [\"name\",\"age\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"warn\"" +
                "}" ) );

        // Tightening to STRICT must now fail due to existing docs missing 'age'
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_strict_addOptionalField_shouldSucceed_andBeEnforcedOnWrites() {
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // Add 'age' but keep required list unchanged (age is optional) -> safe under STRICT.
        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      age:  { type: \"number\" }" +
                "    }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" ) );

        // Existing documents without age still OK
        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\"}", USER ) );

        // But wrong type for age is rejected
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Carl\",\"age\":\"oops\"}", USER ) );
    }


    @Test
    public void alterSchema_strict_tightenNestedAdditionalProperties_withExistingNestedExtras_shouldFail_andNotChangeSchema() {
        createUserCollection( "{" +
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
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        // valid under profile.additionalProperties=true
        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\",\"x\":1}}", USER ) );

        // tighten nested AP to FORBID under STRICT -> must preflight & deny
        assertThrows( Exception.class, () -> alterUserSchema( "{" +
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
                "}" ) );

        // old schema must still be active (nested extra field still allowed)
        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"profile\":{\"first\":\"B\",\"x\":2}}", USER ) );
    }

    @Test
    public void alterSchema_warn_tightenNestedAdditionalProperties_withExistingNestedExtras_shouldSucceed_butTightenToStrictShouldFail() {
        createUserCollection( "{" +
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
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\",\"x\":1}}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{" +
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
                "  validationAction: \"warn\"" +
                "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }

    @Test
    public void alterSchema_patchNestedOptionalField_underStrict_shouldSucceed_andBeEnforcedOnWrites() {
        createUserCollection( "{" +
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

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\"}}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  mode: \"patch\"," +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      profile: {" +
                "        properties: { last: { type: \"text\" } }" +
                "      }" +
                "    }" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" ) );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"profile\":{\"first\":\"B\"}}", USER ) );
        assertDoesNotThrow( () -> insert( "{\"name\":\"Carl\",\"profile\":{\"first\":\"C\",\"last\":\"L\"}}", USER ) );
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Dana\",\"profile\":{\"first\":\"D\",\"last\":1}}", USER ) );
    }

    @Test
    public void alterSchema_patchNestedRequiredField_underWarn_shouldPersist_butTightenToStrictShouldFail() {
        createUserCollection( "{" +
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

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\"}}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  mode: \"patch\"," +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      profile: {" +
                "        properties: { first: { type: \"text\" }, last: { type: \"text\" } }," +
                "        required: [\"first\",\"last\"]" +
                "      }" +
                "    }" +
                "  }," +
                "  validationAction: \"warn\"" +
                "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }

    @Test
    public void alterEnforcementOnly_toStrict_withViolationsAgainstCurrentSchema_shouldFail() {
        // Start with schema persisted but enforcement OFF, so invalid data can exist.
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"off\"" +
                "}" );

        // Missing required field 'name' (should be allowed while OFF)
        assertDoesNotThrow( () -> insert( "{\"age\":42}", USER ) );

        // Tightening enforcement-only to STRICT must preflight current data and deny
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterEnforcementOnly_toStrict_withoutViolations_shouldSucceed_andEnforceOnWrites() {
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    required: [\"name\"]," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"off\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // Tighten to strict should succeed
        assertDoesNotThrow( () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );

        // Now strict should reject invalid writes
        assertThrows( Exception.class, () -> insert( "{\"age\":1}", USER ) );
    }


    @Test
    public void alterValidationAction_withoutPersistedSchema_shouldFail() {
        createUserCollectionWithoutSchema();

        // Cannot set validationAction without an attached schema
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_patchMode_withoutExistingSchema_shouldFail() {
        createUserCollectionWithoutSchema();

        // PATCH only makes sense with a base schema
        assertThrows( Exception.class, () -> alterUserSchema( "{" +
                "  mode: \"patch\"," +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" ) );
    }


    @Test
    public void alterSchema_patchMode_missingAdditionalProperties_shouldBeAllowedForPatch_andPersisted() {
        // Intentionally omit "required" to keep dialect-default (all declared properties required).
        // This makes PATCH-addition of a property become required as well (since required remains null).
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // PATCH: omit top-level additionalProperties -> inherit from current schema wrapper
        // Add a new property "age". With required omitted on both schemas, age becomes required.
        // Keep enforcement WARN so the schema change is allowed even if existing docs violate.
        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  mode: \"patch\"," +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { age: { type: \"text\" } }" +
                "  }," +
                "  validationAction: \"warn\"" +
                "}" ) );

        // If PATCH was applied + persisted, tightening to STRICT must now fail (existing doc missing 'age').
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }

}
