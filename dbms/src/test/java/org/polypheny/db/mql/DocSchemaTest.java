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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * CREATE COLLECTION with docSchema tests.
 *
 * Coverage:
 *  - root object rules (type optional, additionalProperties required)
 *  - required arrays (valid + invalid)
 *  - per-object additionalProperties overrides
 *  - scalar union types ("OR" via type array)
 *  - composition nodes (oneOf/anyOf/allOf/not)
 */
@Tag("adapter")
public class DocSchemaTest extends MqlTestTemplate {

    private static final String USER = "user";

    /**
     * Drop the collection, ignoring failures if it does not exist.
     */
    private void dropUserCollectionIfExists() {
        try {
            dropCollection(USER);
        } catch ( Exception ignored ) {
            // collection might not exist yet; that's fine
        }
    }


    @Test
    public void createCollection_withValidMinimalSchema_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: { name: { type: \"text\" } }," +
                                "    required: [\"name\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_missingRootType_shouldSucceed() {
        // Root "type" is optional (object is inferred / assumed)
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    properties: { name: { type: \"text\" } }," +
                                "    required: [\"name\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_missingAdditionalProperties_shouldFail() {
        // Top-level additionalProperties is required for CREATE/REPLACE.
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: { name: { type: \"text\" } }" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_withUnknownRootType_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"wrongType\"," + // invalid root type
                                "    properties: { name: { type: \"text\" } }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_withUnknownPropertyType_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: { name: { type: \"wrongType\" } }," + // invalid property type
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_withNonObjectProperties_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: \"not-an-object\"," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_withNonObjectDocSchemaLiteral_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: \"just-a-string\"," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_requiredSubset_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
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
                                "})"
                )
        );
    }


    @Test
    public void createCollection_requiredRefersToUndeclaredProperty_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: { name: { type: \"text\" } }," +
                                "    required: [\"name\",\"doesNotExist\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_nestedAdditionalPropertiesOverride_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
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
                                "    required: [\"name\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_invalidNestedAdditionalPropertiesToken_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      profile: {" +
                                "        type: \"object\"," +
                                "        properties: { first: { type: \"text\" } }," +
                                "        additionalProperties: \"banana\"" +
                                "      }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_scalarUnionType_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      nickname: { type: [\"text\",\"null\"] }" +
                                "    }," +
                                "    required: [\"name\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_invalidScalarUnionContainingObject_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: [\"text\",\"object\"] }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_oneOf_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
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
                                "    required: [\"name\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_emptyOneOf_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      contact: { oneOf: [] }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_anyOf_allOf_not_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      status: { anyOf: [ { type: \"text\", const: \"active\" }, { type: \"text\", const: \"pending\" } ] }," +
                                "      score: { allOf: [ { type: \"number\", minimum: 0 }, { type: \"number\", maximum: 10 } ] }," +
                                "      token: { not: { type: \"null\" } }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    /**
     * More complex schema: user has a nested address object and a tags array.
     */
    @Test
    public void createCollection_withNestedAddressSchema_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      address: {" +
                                "        type: \"object\"," +
                                "        properties: {" +
                                "          streetname: { type: \"text\" }," +
                                "          streetno: { type: \"text\" }" +
                                "        }" +
                                "      }," +
                                "      tags: {" +
                                "        type: \"array\"," +
                                "        items: { type: \"text\" }," +
                                "        uniqueItems: true" +
                                "      }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_missingRootType_complexSchema_shouldSucceed() {
        // Root "type" is optional (object inferred from properties)
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      address: {" +
                                "        type: \"object\"," +
                                "        properties: {" +
                                "          streetname: { type: \"text\" }," +
                                "          streetno: { type: \"text\" }" +
                                "        }" +
                                "      }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    @Test
    public void createCollection_withUnknownRootType_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"wrongType\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      address: {" +
                                "        type: \"object\"," +
                                "        properties: {" +
                                "          streetname: { type: \"text\" }," +
                                "          streetno: { type: \"text\" }" +
                                "        }" +
                                "      }," +
                                "      tags: {" +
                                "        type: \"array\"," +
                                "        items: { type: \"text\" }," +
                                "        uniqueItems: true" +
                                "      }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_nestedRequiredRefersToUndeclaredProperty_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      profile: {" +
                                "        type: \"object\"," +
                                "        properties: { first: { type: \"text\" } }," +
                                "        required: [\"last\"]" +
                                "      }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_nestedAdditionalPropertiesInheritToken_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      profile: {" +
                                "        type: \"object\"," +
                                "        properties: { first: { type: \"text\" } }," +
                                "        additionalProperties: \"inherit\"" +
                                "      }" +
                                "    }," +
                                "    required: [\"name\"]," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_emptyAnyOf_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      status: { anyOf: [] }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_emptyAllOf_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      score: { allOf: [] }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_typeArrayEmpty_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      nickname: { type: [] }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_arrayWithoutItems_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      tags: { type: \"array\" }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    @Test
    public void createCollection_withUnknownPropertyType_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"" + USER + "\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      address: {" +
                                "        type: \"object\"," +
                                "        properties: {" +
                                "          streetname: { type: \"text\" }," +
                                "          streetno: { type: \"wrongType\" }" +
                                "        }" +
                                "      }" +
                                "    }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

}
