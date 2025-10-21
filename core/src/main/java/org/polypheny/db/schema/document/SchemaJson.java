/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.schema.document;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON utilities for {@link DocumentSchema}.
 * Provides parsing from a JSON string to a {@code DocumentSchema} and serialization
 * from a {@code DocumentSchema} to its canonical JSON form.
 */
public final class SchemaJson {

    private static final ObjectMapper M = new ObjectMapper();


    private SchemaJson() {
    }


    /**
     * Parses a JSON string into a {@link DocumentSchema}.
     *
     * @param json schema JSON as text
     * @return parsed {@code DocumentSchema}
     * @throws IllegalArgumentException if the input cannot be parsed into a schema
     */
    public static DocumentSchema parse( String json ) {
        try {
            // accept {"root": {...}} or bare {...}
            var node = M.readTree( json );
            if ( node.isTextual() ) {
                node = M.readTree( node.asText() );
            }
            if ( !node.has( "root" ) ) {
                var wrapper = M.createObjectNode();
                wrapper.set( "root", node );
                node = wrapper;
            }
            return M.treeToValue( node, DocumentSchema.class );
        } catch ( Exception e ) {
            throw new IllegalArgumentException( "Invalid DocumentSchema JSON: " + e.getMessage(), e );
        }
    }


    /**
     * Serializes a {@link DocumentSchema} to its canonical JSON representation.
     *
     * @param schema schema instance to serialize
     * @return canonical JSON string
     * @throws IllegalStateException if serialization fails
     */
    public static String toJson( DocumentSchema schema ) {
        try {
            return M.writeValueAsString( schema ); // canonical {"root": {...}}
        } catch ( Exception e ) {
            throw new IllegalStateException( "Failed to serialize DocumentSchema: " + e.getMessage(), e );
        }
    }

}
