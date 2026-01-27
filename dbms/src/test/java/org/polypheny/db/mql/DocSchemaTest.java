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

@Tag("adapter")
public class DocSchemaTest extends MqlTestTemplate {

    /**
     * Drop the collection, ignoring failures if it does not exist.
     */
    private void dropUserCollectionIfExists() {
        try {
            dropCollection( "user" );
        } catch ( Exception ignored ) {
            // collection might not exist yet; that's fine
        }
    }

    /**
     * Correct createCollection query
     */
    @Test
    public void createCollection_withValidMinimalSchema_shouldSucceed() {
        dropUserCollectionIfExists();

        assertDoesNotThrow(
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: { name: { type: \"text\" } }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    /**
     * Root type must be a supported value; an unknown type
     * should cause createCollection to fail.
     */
    @Test
    public void createCollection_withUnknownRootType_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
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

    /**
     * Property type must be supported; "banana" should be rejected.
     */
    @Test
    public void createCollection_withUnknownPropertyType_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
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

    /**
     * Missing "type" on the root docSchema should be rejected.
     * //TODO: IS IT ALLOWED??? --> does not fail, SUCCEEDS!!
     */
    @Test
    public void createCollection_missingRootType_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
                                "    properties: { name: { type: \"text\" } }," +
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    /**
     * "properties" must be an object; using a non-object (e.g. a string)
     * should cause schema validation to fail.
     */
    @Test
    public void createCollection_withNonObjectProperties_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: \"not-an-object\"," + // illegal structure
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

    /**
     * docSchema must itself be an object; passing a literal value instead
     * (e.g. string) should be rejected.
     */
    @Test
    public void createCollection_withNonObjectDocSchemaLiteral_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: \"just-a-string\"," + // invalid: not an object
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
                        "db.createCollection(\"user\", {" +
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

    /**
     * Root type must be a supported value; an unknown type
     * should cause createCollection to fail.
     */
    @Test
    public void createCollection_withUnknownRootType_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
                                "    type: \"wrongType\"," + // invalid root type
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


    /**
     * Property type must be supported; use an invalid type in a nested property.
     */
    @Test
    public void createCollection_withUnknownPropertyType_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: {" +
                                "      name: { type: \"text\" }," +
                                "      address: {" +
                                "        type: \"object\"," +
                                "        properties: {" +
                                "          streetname: { type: \"text\" }," +
                                "          streetno: { type: \"wrongType\" }" + // invalid nested scalar type
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


    /**
     * Missing "type" on the root docSchema.
     */
    @Test
    public void createCollection_missingRootType_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
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


    /**
     * "properties" must be an object; using a non-object (e.g. a string)
     * should cause schema validation to fail.
     */
    @Test
    public void createCollection_withNonObjectProperties_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: {" +
                                "    type: \"object\"," +
                                "    properties: \"not-an-object\"," + // illegal structure
                                "    additionalProperties: true" +
                                "  }," +
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }


    /**
     * docSchema must itself be an object; passing a literal value instead
     * (e.g. string) should be rejected.
     */
    @Test
    public void createCollection_withNonObjectDocSchemaLiteral_complexSchema_shouldFail() {
        dropUserCollectionIfExists();

        assertThrows(
                Exception.class,
                () -> execute(
                        "db.createCollection(\"user\", {" +
                                "  docSchema: \"just-a-string\"," + // invalid: not an object
                                "  validationAction: \"strict\"" +
                                "})"
                )
        );
    }

}
