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
import org.polypheny.db.webui.models.Result;


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
        List<String[]> expected = Arrays.asList(
                new String[]{ "1" },
                new String[]{ "1.3" },
                new String[]{ "test" } );
        insertMany( DATA_0 );

        Result result = aggregate( $project( "{\"test\":1}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }


    @Test
    public void projectMultipleTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ null, "1", "1" },
                new String[]{ "{\"key\":\"val\"}", "1.3", "1.3" },
                new String[]{ "13", "test", "test" } );
        insertMany( DATA_0 );

        Result result = aggregate( $project( "{\"test\":1,\"key\":1}" ), $project( "{\"newName2\":\"$key\",\"newName1\":\"$test\",\"test\":1}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }


    @Test
    public void matchTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":\"test\",\"key\":13}" } );
        insertMany( DATA_0 );

        Result result = aggregate( $match( "{\"test\":\"test\"}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }


    @Test
    public void matchMultipleTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":1}" } );
        insertMany( DATA_0 );

        Result result = aggregate( $match( "{\"$or\":[{\"test\": 1}, {\"test\": 1.3}]}" ), $match( "{\"test\": 1}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }


    @Test
    public void matchProjectTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "val", "1.3" } );
        insertMany( DATA_0 );

        Result result = aggregate( $match( "{\"test\": 1.3}" ), $project( "{\"key.key\":1, \"test\":1}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$addFields


    @Test
    public void addFieldsTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "{\"test\":1,\"added\":52}" },
                new String[]{ "{\"test\":1.3,\"key\":{\"key\":\"val\"},\"added\":52}" },
                new String[]{ "{\"test\":\"test\",\"key\":13,\"added\":52}" } );
        insertMany( DATA_0 );

        Result result = aggregate( $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }


    @Test
    public void projectAddFieldsTest() {
        List<String[]> expected = Arrays.asList(
                new String[]{ "52", "1" },
                new String[]{ "52", "1.3" },
                new String[]{ "52", "test" } );
        insertMany( DATA_0 );

        Result result = aggregate( $project( "{\"test\":1}" ), $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$count


    @Test
    @Category(MonetdbExcluded.class) // MonetClob instead of String
    public void countTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "3" } );
        insertMany( DATA_0 );

        Result result = aggregate( $count( "newName" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$group

    /*@Test
    public void groupNullTest() {
        List<Object[]> expected = ImmutableList.of(
                new String[]{ "3" } );
                insertMany( DATA_0 );

        Result result = aggregate( $group( "{\"_id\": null, \"count\":{\"$avg\":\"$test\"}}" )  );

        MongoConnection.checkResultSet( result, expected );
    }*/


    @Test
    public void groupFieldTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "1" },
                new String[]{ "test" },
                new String[]{ "1.3" } );
        insertMany( DATA_0 );

        Result result = aggregate( $group( "{\"_id\":\"$test\"}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, false );

        expected = ImmutableList.of(
                new String[]{ null },
                new String[]{ "13" },
                new String[]{ "{\"key\":\"val\"}" } );

        result = aggregate( $group( "{\"_id\":\"$key\"}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, false );
    }


    @Test
    public void groupSubFieldTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ null },
                new String[]{ "val" } );
        insertMany( DATA_0 );

        Result result = aggregate( $group( "{\"_id\":\"$key.key\"}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, false );
    }


    @Test
    public void groupAvgTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "val2", "5.0" },
                new String[]{ "val1", "7.0" } );
        insertMany( DATA_1 );

        Result result = aggregate( $group( "{\"_id\":\"$test\", \"avgValue\": {\"$avg\":\"$key\"}}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, false );
    }


    //$limit
    @Test
    @Category(CottontailExcluded.class)
    public void limitTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":\"val1\",\"key\":1}" } );
        insertMany( DATA_1 );

        Result result = aggregate( $limit( 1 ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );

        result = aggregate( $limit( 2 ) );

        expected = ImmutableList.of(
                new String[]{ "{\"test\":\"val1\",\"key\":1}" },
                new String[]{ "{\"test\":\"val2\",\"key\":5}" } );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$replaceRoot


    @Test
    public void replaceRootTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "1" },
                new String[]{ null },
                new String[]{ "13" } );

        insertMany( DATA_2 );

        Result result = aggregate( $replaceRoot( "$test.key" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, false );
    }

    //$replaceWith


    @Test
    public void replaceWithTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "1" },
                new String[]{ null },
                new String[]{ "13" } );

        insertMany( DATA_2 );

        Result result = aggregate( $replaceWith( "$test.key" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, false );
    }

    //$set


    @Test
    public void setTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":1,\"testing\":\"entry\"}" },
                new String[]{ "{\"test\":1.3,\"key\":{\"key\":\"val\"},\"testing\":\"entry\"}" },
                new String[]{ "{\"test\":\"test\",\"key\":13,\"testing\":\"entry\"}" } );

        insertMany( DATA_0 );

        Result result = aggregate( $set( "{\"testing\": \"entry\"}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$skip


    @Test
    @Category(CottontailExcluded.class) // cottontail does not support skips/offset queries
    // without a limit therefore this test cannot be performed correctly using this adapter
    public void skipTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":1.3,\"key\":{\"key\":\"val\"}}" },
                new String[]{ "{\"test\":\"test\",\"key\":13}" } );

        insertMany( DATA_0 );

        Result result = aggregate( $skip( 1 ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );

        expected = ImmutableList.of(
                new String[]{ "{\"test\":\"test\",\"key\":13}" } );

        result = aggregate( $skip( 2 ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$sort


    @Test
    public void sortTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":\"val1\",\"key\":1}" },
                new String[]{ "{\"test\":\"val2\",\"key\":5}" },
                new String[]{ "{\"test\":\"val1\",\"key\":13}" } );

        insertMany( DATA_1 );

        Result result = aggregate( $sort( "{\"key\":1}" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );

        result = aggregate( $sort( "{\"key\":-1}" ) );
        List<String[]> reversed = new ArrayList<>( expected );
        Collections.reverse( reversed );
        MongoConnection.checkUnorderedResultSet( result, reversed, true );
    }

    //$unset


    @Test
    public void unsetTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "{\"test\":\"val1\"}" },
                new String[]{ "{\"test\":\"val2\"}" },
                new String[]{ "{\"test\":\"val1\"}" } );

        insertMany( DATA_1 );

        Result result = aggregate( $unset( "key" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );

        expected = ImmutableList.of(
                new String[]{ "{}" },
                new String[]{ "{}" },
                new String[]{ "{}" } );

        result = aggregate( $unset( Arrays.asList( "\"key\"", "\"test\"" ) ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );
    }

    //$unwind


    @Test
    public void unwindTest() {
        List<String[]> expected = ImmutableList.of(
                new String[]{ "3" },
                new String[]{ "2" },
                new String[]{ "test" },
                new String[]{ "3" },
                new String[]{ "51" },
                new String[]{ "13" } );

        insertMany( DATA_3 );

        Result result = aggregate( $project( "{\"test\":1}" ), $unwind( "$test" ) );

        MongoConnection.checkUnorderedResultSet( result, expected, true );

    }

}
