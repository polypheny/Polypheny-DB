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

package org.polypheny.db.adapter.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DocumentIndexProvider.DocumentIndexMetadata;

class MongoIndexMetadataTest {

    @Test
    void recognizesActualMongoIdIndexAsUnique() {
        assertEquals( List.of( new DocumentIndexMetadata( "_id", "btree", true ) ),
                MongoEntity.describeDocumentIndex( Document.parse( "{\"name\":\"_id_\",\"key\":{\"_id\":1}}" ) ) );
    }


    @Test
    void mapsHashWildcardAndCompoundIndexesWithoutClaimingComponentUniqueness() {
        assertEquals( List.of( new DocumentIndexMetadata( "patient_id", "hash", false ) ),
                MongoEntity.describeDocumentIndex( Document.parse( "{\"key\":{\"patient_id\":\"hashed\"}}" ) ) );
        assertEquals( List.of( new DocumentIndexMetadata( "complications.$**", "wildcard", false ) ),
                MongoEntity.describeDocumentIndex( Document.parse( "{\"key\":{\"complications.$**\":1}}" ) ) );
        assertEquals( List.of( new DocumentIndexMetadata( "patient_id", "btree", false ), new DocumentIndexMetadata( "visit.day", "btree", false ) ),
                MongoEntity.describeDocumentIndex( Document.parse( "{\"unique\":true,\"key\":{\"patient_id\":1,\"visit.day\":-1}}" ) ) );
    }


    @Test
    void ignoresIncompleteIndexMetadata() {
        assertEquals( List.of(), MongoEntity.describeDocumentIndex( new Document( "name", "unknown" ) ) );
    }


    @Test
    void reportsPublicTextPathsInsteadOfMongoInternalFields() {
        assertEquals( List.of( new DocumentIndexMetadata( "notes", "text", false ) ),
                MongoEntity.describeDocumentIndex( Document.parse( "{\"key\":{\"_fts\":\"text\",\"_ftsx\":1},\"weights\":{\"notes\":1}}" ) ) );
    }


    @Test
    void escapesLiteralBackslashesWithoutEscapingNestedPathSeparators() {
        assertEquals( List.of( new DocumentIndexMetadata( "patient\\\\id.visit", "btree", false ) ),
                MongoEntity.describeDocumentIndex( new Document( "key", new Document( "patient\\id.visit", 1 ) ) ) );
    }

}
