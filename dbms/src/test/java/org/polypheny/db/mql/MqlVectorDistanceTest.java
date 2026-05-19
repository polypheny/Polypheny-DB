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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("adapter")
public class MqlVectorDistanceTest extends MqlTestTemplate {

    @BeforeEach
    public void insertData() {
        insert( "{\"name\": \"a\", \"embedding\": [1.0, 1.0]}" );
        insert( "{\"name\": \"b\", \"embedding\": [2.0, 2.0]}" );
        insert( "{\"name\": \"c\", \"embedding\": [0.0, 3.0]}" );
    }


    @Test
    public void l2VectorSearchReturnsResults() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"L2\", \"numCandidates\": 10, \"limit\": 3}}"
        );
        assert res.getData().length == 3;
    }


    @Test
    public void l2VectorSearchOrderedByDistance() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"L2\", \"numCandidates\": 10, \"limit\": 3}}"
        );
        assert res.getData()[0].contains( "\"a\"" );
        assert res.getData()[1].contains( "\"b\"" );
        assert res.getData()[2].contains( "\"c\"" );
    }


    @Test
    public void l1VectorSearchOrderedByDistance() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"L1\", \"numCandidates\": 10, \"limit\": 3}}"
        );
        assert res.getData()[0].contains( "\"a\"" );
    }


    @Test
    public void l2VectorSearchLimit() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"L2\", \"numCandidates\": 10, \"limit\": 1}}"
        );
        assert res.getData().length == 1;
        assert res.getData()[0].contains( "\"a\"" );
    }


    @Test
    public void l2SquaredVectorSearch() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"L2SQUARED\", \"numCandidates\": 10, \"limit\": 3}}"
        );
        assert res.getData()[0].contains( "\"a\"" );
    }


    @Test
    public void chiSquaredVectorSearch() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"CHISQUARED\", \"numCandidates\": 10, \"limit\": 3}}"
        );
        assert res.getData().length == 3;
        assert res.getData()[0].contains( "\"a\"" );
    }


    @Test
    public void cosineVectorSearch() {
        DocResult res = aggregate(
                "{\"$vectorSearch\": {" +
                        "\"path\": \"embedding\", \"queryVector\": [1.0, 1.0], " +
                        "\"metric\": \"COSINE\", \"numCandidates\": 10, \"limit\": 3}}"
        );
        assert res.getData().length == 3;
    }


    @Test
    public void unsupportedMetricThrows() {
        assertThrows( RuntimeException.class, () ->
                MongoConnection.executeGetResponse(
                        "db.test.aggregate([{\"$vectorSearch\": {" +
                                "\"path\":\"embedding\", \"queryVector\": [1.0, 1.0], " +
                                "\"metric\": \"UNKNOWN\", \"numCandidates\": 10, \"limit\": 3}}])"
                )
        );
    }

}
