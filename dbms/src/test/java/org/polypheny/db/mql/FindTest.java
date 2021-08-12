/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.mongoql.model.Result;

@Category(FileExcluded.class)
public class FindTest extends MqlTestTemplate {


    private final List<String> DATA_0 = Arrays.asList(
            "{\"test\":1}",
            "{\"test\":1.3,\"key\":\"val\"}",
            "{\"test\":\"test\",\"key\":13}" );

    private final List<String> DATA_1 = Arrays.asList(
            "{\"test\":{\"sub\":[1,3]}}",
            "{\"test\":{\"sub\":1.3,\"sub2\":[1,23]},\"key\":\"val\"}",
            "{\"test\":\"test\",\"key\":13}" );


    @Test
    public void filterSingleNumberTest() {
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2,\"test2\":13}", "{\"test\":3}" );
        String expected = "{\"test\":2,\"test2\":13}";
        insertMany( data );

        Result result = find( "{\"test2\":13}", "{}" );
        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "id_", expected } ) ) );

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
                        ImmutableList.of( new Object[]{ "id_", expected } ) ) );

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
                                .map( e -> new String[]{ "_id", e } )
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
                        expected
                                .stream()
                                .map( e -> new String[]{ "_id", e } )
                                .collect( Collectors.toList() ), true ) );

    }


    @Test
    public void projectExcludeIdTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "{\"test\":1}" },
                new String[]{ "{\"test\":1.3,\"key\":\"val\"}" },
                new String[]{ "{\"test\":\"test\",\"key\":13}" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"_id\":0}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected, false ) );
    }


    @Test
    public void projectExcludeTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "id_", "{}" },
                new String[]{ "id_", "{\"key\":\"val\"}" },
                new String[]{ "id_", "{\"key\":13}" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"test\":0}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected,
                        true ) );
    }


    @Test
    public void projectExcludeSubTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "id_", "{\"test\":{}}" },
                new String[]{ "id_", "{\"test\":{\"sub2\":[1,23]},\"key\":\"val\"}" },
                new String[]{ "id_", "{\"test\":\"test\",\"key\":13}" } );
        insertMany( DATA_1 );

        Result result = find( "{}", "{\"test.sub\":0}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected, true ) );
    }


    @Test
    public void projectMultipleTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "_id", "1", null },
                new String[]{ "_id", "1.3", "val" },
                new String[]{ "_id", "test", "13" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"test\":1,\"key\":1}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected, true ) );
    }


    @Test
    public void projectRenameTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "_id", "1", null, "1" },
                new String[]{ "_id", "1.3", "val", "1.3" },
                new String[]{ "_id", "test", "13", "test" } );
        insertMany( DATA_0 );

        Result result = find( "{}", "{\"test\":1,\"key\":1,\"newName\":\"$test\"}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        expected, true ) );
    }

    // eq


    @Test
    public void eqTest() {
        insertMany( DATA_0 );
        // if both are active old result is returned TODO dl fix
        /*Result result = find( "{\"test\":{\"$eq\":1}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "id_", "{\"test\":1}" } ) ) );*/

        Result result1 = find( "{\"test\":{\"$eq\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result1,
                        ImmutableList.of( new Object[]{ "id_", "{\"test\":1.3,\"key\":\"val\"}" } ) ) );
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
                                new String[]{ "id_", "{\"test\":1}" },
                                new String[]{ "id_", "{\"test\":\"test\",\"key\":13}" } ), true ) );
    }

    // gt


    @Test
    public void gtTestEmpty() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$gt\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of() ) );
    }


    @Test
    public void gtTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$gt\": 0.9}}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        ImmutableList.of(
                                new String[]{ "_id", "{\"test\":1}" },
                                new String[]{ "_id", "{\"test\":1.3,\"key\":\"val\"}" }
                        ), true ) );
    }


    @Test
    public void gteTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$gte\": 1.3}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of(
                                new Object[]{ "_id", "{\"test\":1.3,\"key\":\"val\"}" }
                        ) ) );
    }

    // lt


    @Test
    public void ltTestEmpty() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$lt\": 1.0}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of() ) );
    }


    @Test
    public void ltTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$lt\": 1.1}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "_id", "{\"test\":1}" } ) ) );
    }


    @Test
    public void lteTest() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$lte\": 1.0}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ "_id", "{\"test\":1}" } ) ) );
    }


    // in
    @Test
    public void inTestEmpty() {
        insertMany( DATA_0 );

        Result result = find( "{\"test\":{\"$in\": [16, \"key\"]}}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of() ) );
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
                                new String[]{ "_id", "{\"test\":1.3,\"key\":\"val\"}" },
                                new String[]{ "_id", "{\"test\":\"test\",\"key\":13}" }
                        ), true ) );
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
                                new Object[]{ "_id", "{\"test\":1}" }
                        ) ) );
    }

    // exits

    // type

    // and

    // not

    // nor

    // or

    // expr

    // mod

    // regex

    // all

    // elemMatch

    // size


}
