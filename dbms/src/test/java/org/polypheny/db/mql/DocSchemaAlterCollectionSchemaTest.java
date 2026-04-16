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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Covers ALTER COLLECTION SCHEMA behavior for strict and warn modes, preflight checks,
 * required-field changes, additionalProperties tightening, patch mode, and enforcement-only changes.
 */
@Tag("adapter")
public class DocSchemaAlterCollectionSchemaTest extends MqlTestTemplate {

    private static final String USER = "user_alter_schema";


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
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"age\":1}", USER ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: false"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" ) );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"age\":2}", USER ) );
    }


    @Test
    public void alterSchema_warn_forbidExtras_withExistingExtras_shouldSucceed_butTightenToStrictShouldFail() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"age\":1}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: false"
                + "  },"
                + "  validationAction: \"warn\""
                + "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_strict_addRequiredField_withExistingDocsMissingField_shouldFail_andNotChangeSchema() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      age:  { type: \"number\" }"
                + "    },"
                + "    required: [\"name\",\"age\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" ) );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\"}", USER ) );
    }


    @Test
    public void alterSchema_warn_addRequiredField_shouldSucceed_butTightenToStrictShouldFail() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      age:  { type: \"number\" }"
                + "    },"
                + "    required: [\"name\",\"age\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"warn\""
                + "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_strict_addOptionalField_shouldSucceed_andBeEnforcedOnWrites() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      age:  { type: \"number\" }"
                + "    },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" ) );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\"}", USER ) );
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Carl\",\"age\":\"oops\"}", USER ) );
    }


    @Test
    public void alterSchema_strict_tightenNestedAdditionalProperties_withExistingNestedExtras_shouldFail_andNotChangeSchema() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      profile: {"
                + "        type: \"object\","
                + "        properties: { first: { type: \"text\" } },"
                + "        required: [\"first\"],"
                + "        additionalProperties: true"
                + "      }"
                + "    },"
                + "    required: [\"name\",\"profile\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\",\"x\":1}}", USER ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      profile: {"
                + "        type: \"object\","
                + "        properties: { first: { type: \"text\" } },"
                + "        required: [\"first\"],"
                + "        additionalProperties: false"
                + "      }"
                + "    },"
                + "    required: [\"name\",\"profile\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" ) );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"profile\":{\"first\":\"B\",\"x\":2}}", USER ) );
    }


    @Test
    public void alterSchema_warn_tightenNestedAdditionalProperties_withExistingNestedExtras_shouldSucceed_butTightenToStrictShouldFail() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      profile: {"
                + "        type: \"object\","
                + "        properties: { first: { type: \"text\" } },"
                + "        required: [\"first\"],"
                + "        additionalProperties: true"
                + "      }"
                + "    },"
                + "    required: [\"name\",\"profile\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\",\"x\":1}}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      profile: {"
                + "        type: \"object\","
                + "        properties: { first: { type: \"text\" } },"
                + "        required: [\"first\"],"
                + "        additionalProperties: false"
                + "      }"
                + "    },"
                + "    required: [\"name\",\"profile\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"warn\""
                + "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_patchNestedOptionalField_underStrict_shouldSucceed_andBeEnforcedOnWrites() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      profile: {"
                + "        type: \"object\","
                + "        properties: { first: { type: \"text\" } },"
                + "        required: [\"first\"],"
                + "        additionalProperties: false"
                + "      }"
                + "    },"
                + "    required: [\"name\",\"profile\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\"}}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  mode: \"patch\","
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      profile: {"
                + "        properties: { last: { type: \"text\" } }"
                + "      }"
                + "    }"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" ) );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Bob\",\"profile\":{\"first\":\"B\"}}", USER ) );
        assertDoesNotThrow( () -> insert( "{\"name\":\"Carl\",\"profile\":{\"first\":\"C\",\"last\":\"L\"}}", USER ) );
        assertThrows( Exception.class, () -> insert( "{\"name\":\"Dana\",\"profile\":{\"first\":\"D\",\"last\":1}}", USER ) );
    }


    @Test
    public void alterSchema_patchNestedRequiredField_underWarn_shouldPersist_butTightenToStrictShouldFail() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      name: { type: \"text\" },"
                + "      profile: {"
                + "        type: \"object\","
                + "        properties: { first: { type: \"text\" } },"
                + "        required: [\"first\"],"
                + "        additionalProperties: false"
                + "      }"
                + "    },"
                + "    required: [\"name\",\"profile\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\",\"profile\":{\"first\":\"A\"}}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  mode: \"patch\","
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: {"
                + "      profile: {"
                + "        properties: { first: { type: \"text\" }, last: { type: \"text\" } },"
                + "        required: [\"first\",\"last\"]"
                + "      }"
                + "    }"
                + "  },"
                + "  validationAction: \"warn\""
                + "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterEnforcementOnly_toStrict_withViolationsAgainstCurrentSchema_shouldFail() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"off\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"age\":42}", USER ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterEnforcementOnly_toStrict_withoutViolations_shouldSucceed_andEnforceOnWrites() {
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    required: [\"name\"],"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"off\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );
        assertDoesNotThrow( () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
        assertThrows( Exception.class, () -> insert( "{\"age\":1}", USER ) );
    }


    @Test
    public void alterValidationAction_withoutPersistedSchema_shouldFail() {
        createUserCollectionWithoutSchema();

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }


    @Test
    public void alterSchema_patchMode_withoutExistingSchema_shouldFail() {
        createUserCollectionWithoutSchema();

        assertThrows( Exception.class, () -> alterUserSchema( "{"
                + "  mode: \"patch\","
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" ) );
    }


    @Test
    public void alterSchema_patchMode_missingAdditionalProperties_shouldBeAllowedForPatch_andPersisted() {
        /**
         * Both schemas omit required, so the dialect default keeps all declared properties required.
         * Adding age through PATCH therefore makes existing documents violate the merged schema.
         */
        createUserCollection( "{"
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { name: { type: \"text\" } },"
                + "    additionalProperties: true"
                + "  },"
                + "  validationAction: \"strict\""
                + "}" );

        assertDoesNotThrow( () -> insert( "{\"name\":\"Alice\"}", USER ) );

        assertDoesNotThrow( () -> alterUserSchema( "{"
                + "  mode: \"patch\","
                + "  docSchema: {"
                + "    type: \"object\","
                + "    properties: { age: { type: \"text\" } }"
                + "  },"
                + "  validationAction: \"warn\""
                + "}" ) );

        assertThrows( Exception.class, () -> alterUserSchema( "{ validationAction: \"strict\" }" ) );
    }

}