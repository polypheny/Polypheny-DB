/*
 * Copyright 2019-2024 The Polypheny Project
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


import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
public class FindTest extends MqlTestTemplate {

    private final List<String> DATA_0 = Arrays.asList(
            "{\"test\":1}",
            "{\"test\":1.3,\"key\":\"val\"}",
            "{\"test\":\"test\",\"key\":13}" );

    private final List<String> DATA_1 = Arrays.asList(
            "{\"test\":{\"sub\":[1,3]}}",
            "{\"test\":{\"sub\":1.3,\"sub2\":[1,23]},\"key\":\"val\"}",
            "{\"test\":\"test\",\"key\":13}" );

    private final List<String> DATA_3 = Arrays.asList(
            "{\"test\":1, \"key\": 2}",
            "{\"test\":2, \"key\": 1}",
            "{\"test\":\"test\",\"key\":13}" );

    private final List<String> DATA_4 = Arrays.asList(
            "{\"test\":\"test2\", \"key\": 3}",
            "{\"test\":\"T1\", \"key\": 2}",
            "{\"test\":\"t1\", \"key\": 2.3}",
            "{\"test\":\"test\", \"key\": 1.1}" );

    private final List<String> DATA_5 = Arrays.asList(
            "{\"test\": [3, 1, \"test\"], \"key\": 3}",
            "{\"test\": [\"test\"], \"key\": 2}",
            "{\"test\": [3,1], \"key\": 2}" );


    @Test
    public void filterSingleNumberTest() {
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2,\"test2\":13}", "{\"test\":3}" );
        String expected = "{\"test\":2,\"test2\":13}";
        insertMany( data );

        DocResult result = find( "{\"test2\":13}", "{}" );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( expected ),
                        true,
                        true ) );
    }


    @Test
    public void filterSubDocumentTest() {
        String expected = "{\"test\":{\"sub\":18},\"test2\":13}";
        List<String> data = Arrays.asList( "{\"test\":1}", expected, "{\"test\":3}" );

        insertMany( data );

        DocResult result = find( "{\"test.sub\":18}", "{}" );
        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( expected ),
                        true,
                        true ) );
    }


    @Test
    public void filterSwitchTest() {
        String expected = "{\"test\":\"test\",\"key\":13}";

        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":15}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(),
                        true,
                        false ) );

        result = find( "{\"test\":\"test\"}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( expected ),
                        true,
                        false ) );
    }


    @Test
    public void projectSingleNumbersTest() {
        List<String> expected = Arrays.asList( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2,\"test2\":13}", "{\"test\":3}" );
        insertMany( data );

        DocResult result = find( "{}", "{\"test\":1}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        expected,
                        true,
                        true ) );
    }


    @Test
    public void projectSingleTypesTest() {
        List<String> expected = Arrays.asList( "{\"test\":1}", "{\"test\":\"test\"}", "{\"test\":3.6}", "{\"test\":10.1}" );
        List<String> data = Arrays.asList(
                "{\"test\":1,\"key\":\"val\"}",
                "{\"test\":\"test\",\"test2\":13}",
                "{\"test\":3.6}",
                "{\"test\":NumberDecimal(\"10.1\")}" );
        insertMany( data );

        DocResult result = find( "{}", "{\"test\":1}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        expected,
                        true,
                        true ) );
    }


    @Test
    public void projectExcludeIdTest() {
        List<String> expected = Arrays.asList(
                "{\"test\":1}",
                "{\"test\":1.3,\"key\":\"val\"}",
                "{\"test\":\"test\",\"key\":13}" );
        insertMany( DATA_0 );

        DocResult result = find( "{}", "{\"_id\":0}" );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, false, true ) );
    }


    @Test
    public void projectExcludeTest() {
        List<String> expected = Arrays.asList(
                "{}",
                "{\"key\":\"val\"}",
                "{\"key\":13}" );
        insertMany( DATA_0 );

        DocResult result = find( "{}", "{\"test\":0}" );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    @Test
    public void projectExcludeSubTest() {
        List<String> expected = Arrays.asList(
                "{\"test\":{}}",
                "{\"test\":{\"sub2\":[1,23]},\"key\":\"val\"}",
                "{\"test\":\"test\",\"key\":13}" );
        insertMany( DATA_1 );

        DocResult result = find( "{}", "{\"test.sub\":0}" );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    @Test
    public void projectMultipleTest() {
        List<String> expected = MongoConnection.arrayToDoc( Arrays.asList(
                        new Object[]{ 1 },
                        new Object[]{ 1.3, "val" },
                        new Object[]{ "test", 13 } ),
                "test",
                "key" );
        insertMany( DATA_0 );

        DocResult result = find( "{}", "{\"test\":1,\"key\":1}" );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }


    @Test
    public void projectRenameTest() {
        List<String> expected = MongoConnection.arrayToDoc( Arrays.asList(
                new Object[]{ 1, 1 },
                new Object[]{ 1.3, 1.3, "val" },
                new Object[]{ "test", "test", 13 } ), "test", "newName", "key" );
        insertMany( DATA_0 );

        DocResult result = find( "{}", "{\"test\":1,\"key\":1,\"newName\":\"$test\"}" );

        assertTrue( MongoConnection.checkDocResultSet( result, expected, true, true ) );
    }

    // eq


    @Test
    public void eqTest() {
        insertMany( DATA_0 );
        // if both are active old DocResult is returned
        DocResult result = find( "{\"test\":{\"$eq\":1}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );

        result = find( "{\"test\":{\"$eq\": \"test\"}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":\"test\",\"key\":13}" ),
                        true,
                        true ) );

        result = find( "{\"test\":{\"$eq\": 1 }}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );
    }

    // ne


    @Test
    public void neTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$ne\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1}",
                                "{\"test\":\"test\",\"key\":13}" ),
                        true, true ) );
    }

    // gt


    @Test
    public void gtTestEmpty() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$gt\": 1.3}}", "{}" );

        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), false, false ) );
    }


    @Test
    public void gtTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$gt\": 0.9}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1}",
                                "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );
    }


    @Test
    public void gteTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$gte\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );
    }

    // lt


    @Test
    public void ltTestEmpty() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$lt\": 1.0}}", "{}" );

        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), true, false ) );
    }


    @Test
    public void ltTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$lt\": 1.1}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );
    }


    @Test
    public void lteTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$lte\": 1.0}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );
    }


    // in
    @Test
    public void inTestEmpty() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$in\": [16, \"key\"]}}", "{}" );

        assertTrue( MongoConnection.checkDocResultSet( result, ImmutableList.of(), true, true ) );
    }


    // in
    @Test
    public void inTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$in\": [1.3, \"test\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1.3,\"key\":\"val\"}",
                                "{\"test\":\"test\",\"key\":13}" ),
                        true,
                        true ) );
    }


    // nin
    @Test
    public void ninTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$nin\": [1.3, \"test\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );
    }


    // exits
    @Test
    public void existsTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"test\":{\"$exists\": true}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1}",
                                "{\"test\":1.3,\"key\":\"val\"}",
                                "{\"test\":\"test\",\"key\":13}" ),
                        true,
                        true ) );

        result = find( "{\"key\":{\"$exists\": true}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1.3,\"key\":\"val\"}",
                                "{\"test\":\"test\",\"key\":13}" ),
                        true,
                        true ) );

        result = find( "{\"key\":{\"$exists\": false}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1}" ),
                        true,
                        true ) );
    }


    // type
    @Test
    public void typeTest() {
        insertMany( DATA_0 );

        // 2 is String
        DocResult result = find( "{\"key\":{\"$type\": 2}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );

        result = find( "{\"key\":{\"$type\": \"string\"}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );

        // 1 is double
        result = find( "{\"test\":{\"$type\": 1}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );

        result = find( "{\"test\":{\"$type\": \"number\"}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1}",
                                "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );
    }

    // and


    @Test
    public void andTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"$and\": [ {\"key\": {\"$exists\": true}}, {\"key\": {\"$type\": 2}} ]}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );

        // implicit $and
        result = find( "{\"key\": {\"$exists\": true}, \"key\": {\"$type\": 2}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1.3,\"key\":\"val\"}" ),
                        true,
                        true ) );
    }


    // not
    @Test
    public void notTest() {
        insertMany( DATA_0 );

        DocResult result = find( "{\"key\": { \"$not\": [{\"$exists\": true}]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );
    }


    // nor
    @Test
    public void norTest() {
        insertMany( DATA_0 );

        // neither double nor string
        DocResult result = find( "{\"$nor\": [ {\"test\": {\"$type\": 2}}, {\"test\": {\"$type\": 1}} ]}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":1}" ),
                        true,
                        true ) );
    }


    // or
    @Test
    public void orTest() {
        insertMany( DATA_0 );

        // neither double nor string
        DocResult result = find( "{\"$or\": [ {\"test\": {\"$type\": 2}}, {\"test\": {\"$type\": 1}} ]}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":1.3,\"key\":\"val\"}",
                                "{\"test\":\"test\",\"key\":13}" ),
                        true, true ) );
    }


    // expr
    @Test
    public void exprTest() {
        insertMany( DATA_3 );

        DocResult result = find( "{\"test\": {\"$type\": \"number\"}, \"key\": {\"$type\": \"number\"},\"$expr\":{ \"$gt\": [\"$test\",  \"$key\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\":2, \"key\": 1}" ),
                        true,
                        true ) );
    }

    // mod


    @Test
    public void modTest() {
        insertMany( DATA_3 );

        DocResult result = find( "{\"key\": {\"$mod\": [2, 1]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":2, \"key\": 1}",
                                "{\"test\":\"test\",\"key\":13}" ),
                        true,
                        true ) );
    }

    // regex


    @Test
    public void regexTest() {
        insertMany( DATA_4 );

        DocResult result = find( "{\"test\": {\"$regex\": 'test'}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":\"test2\", \"key\": 3}",
                                "{\"test\":\"test\", \"key\": 1.1}" ),
                        true,
                        true ) );

        result = find( "{\"test\": {\"$regex\": 't1', \"$options\": \"i\"}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\":\"T1\", \"key\": 2}",
                                "{\"test\":\"t1\", \"key\": 2.3}" ),
                        true,
                        true ) );
    }

    // all


    @Test
    public void allTest() {
        insertMany( DATA_5 );

        DocResult result = find( "{\"test\": {\"$all\": [1,3]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\": [3, 1, \"test\"], \"key\": 3}",
                                "{\"test\": [3,1], \"key\": 2}" ),
                        true,
                        true ) );

        result = find( "{\"test\": {\"$all\": [\"test\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\": [3, 1, \"test\"], \"key\": 3}",
                                "{\"test\": [\"test\"], \"key\": 2}" ),
                        true,
                        true ) );
    }


    // elemMatch
    @Test
    public void elemMatchTest() {
        insertMany( DATA_5 );

        DocResult result = find( "{\"test\": {\"$elemMatch\": {\"$gt\": 2}}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(
                                "{\"test\": [3, 1, \"test\"], \"key\": 3}",
                                "{\"test\": [3, 1], \"key\": 2}" ),
                        true,
                        true ) );
    }

    // size


    @Test
    public void sizeTest() {
        insertMany( DATA_5 );

        DocResult result = find( "{\"test\": {\"$size\": 1}}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( "{\"test\": [\"test\"], \"key\": 2}" ),
                        true,
                        true ) );
    }

}
