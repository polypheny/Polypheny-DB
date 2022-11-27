/*
 * Copyright 2019-2022 The Polypheny Project
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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.webui.models.Result;


@Category({ AdapterTestSuite.class, FileExcluded.class, CassandraExcluded.class })
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

        Result result = find( "{\"test2\":13}", "{}" );
        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ expected } ), true ) );
    }


    @Test
    public void filterSubDocumentTest() {
        String expected = "{\"test\":{\"sub\":18},\"test2\":13}";
        List<String> data = Arrays.asList( "{\"test\":1}", expected, "{\"test\":3}" );

        insertMany( data );

        Result result = find( "{\"test.sub\":18}", "{}" );
        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ expected } ), true ) );
    }


    @Test
    public void filterSwitchTest() {
        String expected = "{\"test\":\"test\",\"key\":13}";

        insertMany( DATA_0 );

        Result result = find( "{\"test\":15}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(), true ) );

        result = find( "{\"test\":\"test\"}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ expected } ), true ) );
    }


    @Test
    public void projectSingleNumbersTest() {
        List<String> expected = Arrays.asList( "1", "2", "3" );
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2,\"test2\":13}", "{\"test\":3}" );
        insertMany( data );

        Result result = find( "{}", "{\"test\":1}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected
                                .stream()
                                .map( e -> new String[]{ e } )
                                .collect( Collectors.toList() ),
                        true ) );
    }


    @Test
    public void projectSingleTypesTest() {
        List<String> expected = Arrays.asList( "1", "test", "3.6", "10.1" );
        List<String> data = Arrays.asList(
                "{\"test\":1,\"key\":\"val\"}",
                "{\"test\":\"test\",\"test2\":13}",
                "{\"test\":3.6}",
                "{\"test\":NumberDecimal(\"10.1\")}" );
        insertMany( data );

        Result result = find( "{}", "{\"test\":1}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected.stream()
                                .map( e -> new String[]{ e } )
                                .collect( Collectors.toList() ),
                        true ) );
    }


    @Test
    public void projectExcludeIdTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "{\"test\":1}" },
                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" },
                new String[]{ "{\"test\":\"test\",\"key\":13}" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"_id\":0}" );

        assertTrue( MongoConnection.checkUnorderedResultSet( result, expected, false ) );
    }


    @Test
    public void projectExcludeTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "{}" },
                new String[]{ "{\"key\":\"val\"}" },
                new String[]{ "{\"key\":13}" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"test\":0}" );

        assertTrue( MongoConnection.checkUnorderedResultSet( result, expected, true ) );
    }


    @Test
    public void projectExcludeSubTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "{\"test\":{}}" },
                new String[]{ "{\"test\":{\"sub2\":[1,23]},\"key\":\"val\"}" },
                new String[]{ "{\"test\":\"test\",\"key\":13}" } );
        insertMany( DATA_1 );

        Result result = find( "{}", "{\"test.sub\":0}" );

        assertTrue( MongoConnection.checkUnorderedResultSet( result, expected, true ) );
    }


    @Test
    public void projectMultipleTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "1", null },
                new String[]{ "1.3", "val" },
                new String[]{ "test", "13" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"test\":1,\"key\":1}" );

        assertTrue( MongoConnection.checkUnorderedResultSet( result, expected, true ) );
    }


    @Test
    public void projectRenameTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "1", null, "1" },
                new String[]{ "1.3", "val", "1.3" },
                new String[]{ "test", "13", "test" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"test\":1,\"key\":1,\"newName\":\"$test\"}" );

        assertTrue( MongoConnection.checkUnorderedResultSet( result, expected, true ) );
    }

    // eq


    @Test
    public void eqTest() {
        insertMany( DATA_0 );
        // if both are active old result is returned
        Result result = find( "{\"test\":{\"$eq\":1}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "{\"test\":1}" } ), true ) );

        result = find( "{\"test\":{\"$eq\": \"test\"}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "{\"test\":\"test\",\"key\":13}" } ), true ) );

        result = find( "{\"test\":{\"$eq\": 1 }}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "{\"test\":1}" } ), true ) );
    }

    // ne


    @Test
    public void neTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$ne\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" },
                                new String[]{ "{\"test\":\"test\",\"key\":13}" } ),
                        true ) );
    }

    // gt


    @Test
    public void gtTestEmpty() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$gt\": 1.3}}", "{}" );

        assertTrue( MongoConnection.checkResultSet( result, ImmutableList.of(), false ) );
    }


    @Test
    public void gtTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$gt\": 0.9}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" },
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ),
                        true ) );
    }


    @Test
    public void gteTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$gte\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new Object[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );
    }

    // lt


    @Test
    public void ltTestEmpty() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$lt\": 1.0}}", "{}" );

        assertTrue( MongoConnection.checkResultSet( result, ImmutableList.of(), true ) );
    }


    @Test
    public void ltTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$lt\": 1.1}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "{\"test\":1}" } ), true ) );
    }


    @Test
    public void lteTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$lte\": 1.0}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "{\"test\":1}" } ), true ) );
    }


    // in
    @Test
    public void inTestEmpty() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$in\": [16, \"key\"]}}", "{}" );

        assertTrue( MongoConnection.checkResultSet( result, ImmutableList.of(), true ) );
    }


    // in
    @Test
    public void inTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$in\": [1.3, \"test\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" },
                                new String[]{ "{\"test\":\"test\",\"key\":13}" }
                        ),
                        true ) );
    }


    // nin
    @Test
    public void ninTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$nin\": [1.3, \"test\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new Object[]{ "{\"test\":1}" }
                        ), true ) );
    }


    // exits
    @Test
    public void existsTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$exists\": true}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" },
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" },
                                new String[]{ "{\"test\":\"test\",\"key\":13}" }
                        ),
                        true ) );

        result = find( "{\"key\":{\"$exists\": true}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" },
                                new String[]{ "{\"test\":\"test\",\"key\":13}" }
                        ),
                        true ) );

        result = find( "{\"key\":{\"$exists\": false}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" }
                        ), true ) );
    }


    // type
    @Test
    public void typeTest() {
        insertMany( DATA_0 );

        // 2 is String
        Result result = find( "{\"key\":{\"$type\": 2}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );

        result = find( "{\"key\":{\"$type\": \"string\"}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );

        // 1 is double
        result = find( "{\"test\":{\"$type\": 1}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );

        result = find( "{\"test\":{\"$type\": \"number\"}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" },
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ),
                        true ) );
    }

    // and


    @Test
    public void andTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"$and\": [ {\"key\": {\"$exists\": true}}, {\"key\": {\"$type\": 2}} ]}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );

        // implicit $and
        result = find( "{\"key\": {\"$exists\": true}, \"key\": {\"$type\": 2}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );
    }


    // not
    @Test
    public void notTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"key\": { \"$not\": {\"$exists\": true}}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" }
                        ), true ) );
    }


    // nor
    @Test
    public void norTest() {
        insertMany( DATA_0 );

        // neither double nor string
        Result result = find( "{\"$nor\": [ {\"test\": {\"$type\": 2}}, {\"test\": {\"$type\": 1}} ]}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":1}" }
                        ), true ) );
    }


    // or
    @Test
    public void orTest() {
        insertMany( DATA_0 );

        // neither double nor string
        Result result = find( "{\"$or\": [ {\"test\": {\"$type\": 2}}, {\"test\": {\"$type\": 1}} ]}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(

                                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" },
                                new String[]{ "{\"test\":\"test\",\"key\":13}" }
                        ),
                        true ) );
    }


    // expr
    @Test
    public void exprTest() {
        insertMany( DATA_3 );

        Result result = find( "{\"test\": {\"$type\": \"number\"}, \"key\": {\"$type\": \"number\"},\"$expr\":{ \"$gt\": [\"$test\",  \"$key\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":2, \"key\": 1}" }
                        ), true ) );
    }

    // mod


    @Test
    public void modTest() {
        insertMany( DATA_3 );

        Result result = find( "{\"key\": {\"$mod\": [2, 1]}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":2, \"key\": 1}" },
                                new String[]{ "{\"test\":\"test\",\"key\":13}" }
                        ),
                        true ) );
    }

    // regex


    @Test
    public void regexTest() {
        insertMany( DATA_4 );

        Result result = find( "{\"test\": {\"$regex\": 'test'}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":\"test2\", \"key\": 3}" },
                                new String[]{ "{\"test\":\"test\", \"key\": 1.1}" }
                        ), true ) );

        result = find( "{\"test\": {\"$regex\": 't1', \"$options\": \"i\"}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\":\"T1\", \"key\": 2}" },
                                new String[]{ "{\"test\":\"t1\", \"key\": 2.3}" }
                        ),
                        true ) );
    }

    // all


    @Test
    public void allTest() {
        insertMany( DATA_5 );

        Result result = find( "{\"test\": {\"$all\": [1,3]}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\": [3, 1, \"test\"], \"key\": 3}" },
                                new String[]{ "{\"test\": [3,1], \"key\": 2}" }
                        ),
                        true ) );

        result = find( "{\"test\": {\"$all\": [\"test\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\": [3, 1, \"test\"], \"key\": 3}" },
                                new String[]{ "{\"test\": [\"test\"], \"key\": 2}" }
                        ),
                        true ) );
    }


    // elemMatch
    @Test
    public void elemMatchTest() {
        insertMany( DATA_5 );

        Result result = find( "{\"test\": {\"$elemMatch\": {\"$gt\": 2}}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\": [3, 1, \"test\"], \"key\": 3}" },
                                new String[]{ "{\"test\": [3, 1], \"key\": 2}" }
                        ),
                        true ) );
    }

    // size


    @Test
    public void sizeTest() {
        insertMany( DATA_5 );

        Result result = find( "{\"test\": {\"$size\": 1}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "{\"test\": [\"test\"], \"key\": 2}" }
                        ), true ) );
    }

}
