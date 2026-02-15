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

import org.junit.jupiter.api.AfterEach;
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
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // Adding a declared property makes it required in this dialect.
        assertThrows( Exception.class, () -> alterUserSchema( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: {" +
                "      name: { type: \"text\" }," +
                "      age:  { type: \"text\" }" +
                "    }," +
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
                "      age:  { type: \"text\" }" +
                "    }," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"warn\"" +
                "}" ) );

        // Tightening to STRICT must now fail due to existing docs missing 'age'
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterEnforcementOnly_toStrict_withViolationsAgainstCurrentSchema_shouldFail() {
        // Start with schema persisted but enforcement OFF, so invalid data can exist.
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
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
        createUserCollection( "{" +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { name: { type: \"text\" } }," +
                "    additionalProperties: true" +
                "  }," +
                "  validationAction: \"strict\"" +
                "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        // PATCH: omit additionalProperties -> inherit from current schema
        // Add a new required field, but keep enforcement WARN so it's allowed even if existing docs violate.
        assertDoesNotThrow( () -> alterUserSchema( "{" +
                "  mode: \"patch\"," +
                "  docSchema: {" +
                "    type: \"object\"," +
                "    properties: { age: { type: \"text\" } }" +
                "    additionalProperties: false" +
                "  }," +
                "  validationAction: \"warn\"" +
                "}" ) );

        // If PATCH was applied + persisted, tightening to STRICT must now fail (existing doc missing 'age').
        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }

}
