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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.prism.Document;
import org.polypheny.prism.DocumentField;

class DocumentMetaRetrieverTest {

    @BeforeAll
    static void initializeValueSerialization() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }

    private static PolyDocument document( String json ) {
        return PolyValue.fromJson( json ).asDocument();
    }


    @Test
    void describesEmptyCollectionsWithoutInventingFieldsOrSample() {
        Document metadata = DocumentMetaRetriever.describeDocuments( "patients", "symptoms", List.of() ).build();
        assertEquals( "patients", metadata.getNamespaceName() );
        assertEquals( "symptoms", metadata.getCollectionName() );
        assertEquals( 0, metadata.getFieldsCount() );
        assertEquals( 0, metadata.getDocumentIndexesCount() );
        assertFalse( metadata.hasSampleDocumentJson() );
    }


    @Test
    void infersNestedFieldsNullabilityAndTypesAcrossDocuments() {
        PolyDocument first = document( "{\"patient_id\":\"p1\",\"age\":42,\"viral_load\":6,\"active\":true,"
                + "\"tags\":[\"fever\"],\"complications\":{\"renal\":true},\"nullable\":null,\"missing\":1,\"mixed\":1}" );
        PolyDocument second = document( "{\"patient_id\":\"p2\",\"age\":39,\"viral_load\":6.5,\"active\":false,"
                + "\"tags\":[],\"complications\":null,\"nullable\":\"known\",\"mixed\":\"severe\",\"only_null\":null}" );
        Document metadata = DocumentMetaRetriever.describeDocuments( "patients", "symptoms", List.of( first, second ) ).build();
        Map<String, DocumentField> fields = metadata.getFieldsList().stream().collect( Collectors.toMap( DocumentField::getFieldPath, Function.identity() ) );

        assertEquals( "string", fields.get( "patient_id" ).getTypeName() );
        assertFalse( fields.get( "patient_id" ).getIsOptional() );
        assertEquals( "int", fields.get( "age" ).getTypeName() );
        assertEquals( "double", fields.get( "viral_load" ).getTypeName() );
        assertEquals( "bool", fields.get( "active" ).getTypeName() );
        assertEquals( "array", fields.get( "tags" ).getTypeName() );
        assertEquals( "object", fields.get( "complications" ).getTypeName() );
        assertTrue( fields.get( "complications" ).getIsOptional() );
        assertEquals( "bool", fields.get( "complications.renal" ).getTypeName() );
        assertTrue( fields.get( "complications.renal" ).getIsOptional() );
        assertEquals( "string", fields.get( "nullable" ).getTypeName() );
        assertTrue( fields.get( "nullable" ).getIsOptional() );
        assertTrue( fields.get( "missing" ).getIsOptional() );
        assertEquals( "mixed", fields.get( "mixed" ).getTypeName() );
        assertEquals( "null", fields.get( "only_null" ).getTypeName() );
        assertTrue( fields.get( "only_null" ).getIsOptional() );
        assertTrue( metadata.hasSampleDocumentJson() );
        assertEquals( first, document( metadata.getSampleDocumentJson() ) );
        assertEquals( fields.keySet().stream().sorted().toList(), metadata.getFieldsList().stream().map( DocumentField::getFieldPath ).toList() );
    }


    @Test
    void handlesMissingNestedParentsAndUnsetValues() {
        PolyDocument first = document( "{\"complications\":{\"renal\":{\"onset_day\":2}}}" );
        first.put( PolyString.of( "unset" ), PolyDocument.ofUnset() );
        Document metadata = DocumentMetaRetriever.describeDocuments( "n", "c", List.of( first, document( "{}" ) ) ).build();
        assertEquals( List.of( "complications", "complications.renal", "complications.renal.onset_day" ),
                metadata.getFieldsList().stream().map( DocumentField::getFieldPath ).toList() );
        assertTrue( metadata.getFieldsList().stream().allMatch( DocumentField::getIsOptional ) );
    }


    @Test
    void boundsRecursionForDeeplyNestedDocuments() {
        PolyDocument nested = document( "{\"leaf\":1}" );
        for ( int depth = 0; depth < DocumentMetaRetriever.MAX_FIELD_DEPTH + 2; depth++ ) {
            nested = PolyDocument.ofDocument( Map.of( PolyString.of( "child" ), nested ) );
        }
        Document metadata = DocumentMetaRetriever.describeDocuments( "n", "c", List.of( nested ) ).build();
        assertEquals( DocumentMetaRetriever.MAX_FIELD_DEPTH, metadata.getFieldsCount() );
    }


    @Test
    void distinguishesLiteralDotsAndBackslashesFromNestedPathSeparators() {
        PolyDocument first = document( "{\"a.b\":1,\"a\":{\"b\":\"x\"}}" );
        first.put( PolyString.of( "slash\\key" ), PolyValue.fromJson( "true" ) );
        Document metadata = DocumentMetaRetriever.describeDocuments( "n", "c",
                List.of( first, document( "{\"a\":{\"b\":\"y\"}}" ) ) ).build();
        Map<String, DocumentField> fields = metadata.getFieldsList().stream().collect( Collectors.toMap( DocumentField::getFieldPath, Function.identity() ) );

        assertEquals( 4, fields.size() );
        assertEquals( "int", fields.get( "a\\.b" ).getTypeName() );
        assertTrue( fields.get( "a\\.b" ).getIsOptional() );
        assertEquals( "string", fields.get( "a.b" ).getTypeName() );
        assertFalse( fields.get( "a.b" ).getIsOptional() );
        assertEquals( "bool", fields.get( "slash\\\\key" ).getTypeName() );
        assertTrue( fields.get( "slash\\\\key" ).getIsOptional() );
    }

}
