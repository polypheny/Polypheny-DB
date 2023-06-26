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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.CottontailExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.excluded.MonetdbExcluded;
import org.polypheny.db.webui.models.results.DocResult;


@Category({ AdapterTestSuite.class, FileExcluded.class, CassandraExcluded.class })
public class AggregateTest extends MqlTestTemplate {


    private final List<String> DATA_0 = Arrays.asList(
            "{\"test\":1}",
            "{\"test\":1.3,\"key\":{\"key\": \"val\"}}",
            "{\"test\":\"test\",\"key\":13}" );

    private final List<String> DATA_1 = Arrays.asList(
            "{\"test\":\"val1\",\"key\":1}",
            "{\"test\":\"val2\",\"key\":5}",
            "{\"test\":\"val1\",\"key\":13}" );

    private final List<String> DATA_2 = Arrays.asList(
            "{\"test\":{\"key\":1}}",
            "{\"test\":{\"key1\":5}}",
            "{\"test\":{\"key\":13}}" );

    private final List<String> DATA_3 = Arrays.asList(
            "{\"val\":3,\"test\":[3,2]}",
            "{\"val\":\"31\",\"test\":[\"test\",3,51]}",
            "{\"test\":[13]}" );


    @Test
    public void projectTest() {
        List<String> expected = Arrays.asList(
                "{test: 1}",
                "{test: 1.3}",
                "{test: \"test\"}" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $project( "{\"test\":1}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }


    @Test
    public void projectMultipleTest() {
        List<String> expected = MongoConnection.arrayToDoc( Arrays.asList(
                        new Object[]{ null, 1, 1 },
                        new Object[]{ "{\"key\":\"val\"}", 1.3, 1.3 },
                        new Object[]{ 13, "test", "test" } ),
                "newName2", "newName1", "test" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $project( "{\"test\":1,\"key\":1}" ), $project( "{\"newName2\":\"$key\",\"newName1\":\"$test\",\"test\":1}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }


    @Test
    public void matchTest() {
        List<String> expected = ImmutableList.of( "{\"test\":\"test\",\"key\":13}" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $match( "{\"test\":\"test\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }


    @Test
    public void matchMultipleTest() {
        List<String> expected = ImmutableList.of( "{\"test\":1}" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $match( "{\"$or\":[{\"test\": 1}, {\"test\": 1.3}]}" ), $match( "{\"test\": 1}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }


    @Test
    public void matchProjectTest() {
        List<String> expected = ImmutableList.of( "{ key.key: \"val\", test: 1.3 }" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $match( "{\"test\": 1.3}" ), $project( "{\"key.key\":1, \"test\":1}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$addFields


    @Test
    public void addFieldsTest() {
        List<String> expected = Arrays.asList(
                "{\"test\":1,\"added\":52}",
                "{\"test\":1.3,\"key\":{\"key\":\"val\"},\"added\":52}",
                "{\"test\":\"test\",\"key\":13,\"added\":52}" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }


    @Test
    public void projectAddFieldsTest() {
        List<String> expected = MongoConnection.arrayToDoc( Arrays.asList(
                        new String[]{ "52", "1" },
                        new String[]{ "52", "1.3" },
                        new String[]{ "52", "test" } ),
                "test",
                "added" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $project( "{\"test\":1}" ), $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$count


    @Test
    @Category(MonetdbExcluded.class) // MonetClob instead of String
    public void countTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                new String[]{ "3" } ), "count" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $count( "newName" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$group

    /*@Test
    public void groupNullTest() {
        List<Object[]> expected = ImmutableList.of(
                new String[]{ "3" } );
                insertMany( DATA_0 );

        Result result = aggregate( $group( "{\"_id\": null, \"count\":{\"$avg\":\"$test\"}}" )  );

        MongoConnection.checkDocResultSet( result, expected );
    }*/


    @Test
    public void groupFieldTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                new String[]{ "1" },
                new String[]{ "test" },
                new String[]{ "1.3" } ), "test" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $group( "{\"_id\":\"$test\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );

        expected = MongoConnection.arrayToDoc( ImmutableList.of(
                new String[]{ null },
                new String[]{ "13" },
                new String[]{ "{\"key\":\"val\"}" } ), "test" );

        result = aggregate( $group( "{\"_id\":\"$key\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    @Test
    public void groupSubFieldTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                        new String[]{ null },
                        new String[]{ "val" } ),
                "_id" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $group( "{\"_id\":\"$key.key\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    @Test
    public void groupAvgTest() {
        List<String> expected = MongoConnection.arrayToDoc( List.of(
                        new Object[]{ "val2", 5 },
                        new Object[]{ "val1", 7 } ),
                "test", "avgValue" );
        insertMany( DATA_1 );

        DocResult result = aggregate( $group( "{\"_id\":\"$test\", \"avgValue\": {\"$avg\":\"$key\"}}" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    //$limit
    @Test
    @Category(CottontailExcluded.class)
    public void limitTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":\"val1\",\"key\":1}" );
        insertMany( DATA_1 );

        DocResult result = aggregate( $limit( 1 ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

        result = aggregate( $limit( 2 ) );

        expected = ImmutableList.of(
                "{\"test\":\"val1\",\"key\":1}",
                "{\"test\":\"val2\",\"key\":5}" );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$replaceRoot


    @Test
    public void replaceRootTest() {
        List<String> expected = Arrays.asList(
                "1",
                null,
                "13" );

        insertMany( DATA_2 );

        DocResult result = aggregate( $replaceRoot( "$test.key" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }

    //$replaceWith


    @Test
    public void replaceWithTest() {
        List<String> expected = Arrays.asList( "1",
                null,
                "13" );

        insertMany( DATA_2 );

        DocResult result = aggregate( $replaceWith( "$test.key" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }

    //$set


    @Test
    public void setTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":1,\"testing\":\"entry\"}",
                "{\"test\":1.3,\"key\":{\"key\":\"val\"},\"testing\":\"entry\"}",
                "{\"test\":\"test\",\"key\":13,\"testing\":\"entry\"}" );

        insertMany( DATA_0 );

        DocResult result = aggregate( $set( "{\"testing\": \"entry\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$skip


    @Test
    @Category(CottontailExcluded.class) // cottontail does not support skips/offset queries
    // without a limit therefore this test cannot be performed correctly using this adapter
    public void skipTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":1.3,\"key\":{\"key\":\"val\"}}",
                "{\"test\":\"test\",\"key\":13}" );

        insertMany( DATA_0 );

        DocResult result = aggregate( $skip( 1 ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

        expected = ImmutableList.of(
                "{\"test\":\"test\",\"key\":13}" );

        result = aggregate( $skip( 2 ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$sort


    @Test
    public void sortTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":\"val1\",\"key\":1}",
                "{\"test\":\"val2\",\"key\":5}",
                "{\"test\":\"val1\",\"key\":13}" );

        insertMany( DATA_1 );

        DocResult result = aggregate( $sort( "{\"key\":1}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

        result = aggregate( $sort( "{\"key\":-1}" ) );
        List<String> reversed = new ArrayList<>( expected );
        Collections.reverse( reversed );
        MongoConnection.checkDocResultSet( result, reversed, true, true );
    }

    //$unset


    @Test
    public void unsetTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":\"val1\"}",
                "{\"test\":\"val2\"}",
                "{\"test\":\"val1\"}" );

        insertMany( DATA_1 );

        DocResult result = aggregate( $unset( "key" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

        expected = ImmutableList.of(
                "{}",
                "{}",
                "{}" );

        result = aggregate( $unset( Arrays.asList( "\"key\"", "\"test\"" ) ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$unwind


    @Test
    public void unwindTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                new String[]{ "3" },
                new String[]{ "2" },
                new String[]{ "test" },
                new String[]{ "3" },
                new String[]{ "51" },
                new String[]{ "13" } ), "test" );

        insertMany( DATA_3 );

        DocResult result = aggregate( $project( "{\"test\":1}" ), $unwind( "$test" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

    }

}
