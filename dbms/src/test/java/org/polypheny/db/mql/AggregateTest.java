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


import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.mongoql.model.Result;


public class AggregateTest extends MqlTestTemplate {


    private final List<String> DATA_0 = Arrays.asList(
            "{\"test\":1}",
            "{\"test\":1.3,\"key\":{\"key\": \"val\"}}",
            "{\"test\":\"test\",\"key\":13}" );

    private final List<String> DATA_1 = Arrays.asList(
            "{\"test\":\"val1\",\"key\":1}",
            "{\"test\":\"val2\",\"key\":5}",
            "{\"test\":\"val1\",\"key\":13}" );


    @Test
    public void projectTest() {
        List<Object[]> expected = Arrays.asList(
                new String[]{ "id_", "1" },
                new String[]{ "id_", "1.3" },
                new String[]{ "id_", "test" } );
        insertMany( DATA_0 );

        Result result = aggregate( $project( "{\"test\":1}" ) );

        MongoConnection.checkResultSet( result, expected );
    }


    @Test
    public void projectMultipleTest() {
        List<Object[]> expected = Arrays.asList(
                new String[]{ "_id", null, "1", "1" },
                new String[]{ "_id", "{\"key\":\"val\"}", "1.3", "1.3" },
                new String[]{ "_id", "13", "test", "test" } );
        insertMany( DATA_0 );

        Result result = aggregate( $project( "{\"test\":1,\"key\":1}" ), $project( "{\"newName2\":\"$key\",\"newName1\":\"$test\",\"test\":1}" ) );

        MongoConnection.checkResultSet( result, expected );
    }


    @Test
    public void matchTest() {
        List<Object[]> expected = ImmutableList.of(
                new String[]{ "id_", "{\"test\":\"test\",\"key\":13}" } );
        insertMany( DATA_0 );

        Result result = aggregate( $match( "{\"test\":\"test\"}" ) );

        MongoConnection.checkResultSet( result, expected );
    }


    @Test
    public void matchMultipleTest() {
        List<Object[]> expected = ImmutableList.of(
                new String[]{ "id_", "{\"test\":1}" } );
        insertMany( DATA_0 );

        Result result = aggregate( $match( "{\"$or\":[{\"test\": 1}, {\"test\": 1.3}]}" ), $match( "{\"test\": 1}" ) );

        MongoConnection.checkResultSet( result, expected );
    }


    @Test
    public void matchProjectTest() {
        List<Object[]> expected = ImmutableList.of(
                new String[]{ "_id", "val", "1.3" } );
        insertMany( DATA_0 );

        Result result = aggregate( $match( "{\"test\": 1.3}" ), $project( "{\"key.key\":1, \"test\":1}" ) );

        MongoConnection.checkResultSet( result, expected );
    }

    //$addFields


    @Test
    public void addFieldsTest() {
        List<Object[]> expected = Arrays.asList(
                new String[]{ "id_", "{\"test\":1, \"added\":52}" },
                new String[]{ "id_", "{\"test\":1.3,\"key\":{\"key\": \"val\"},\"added\":52}" },
                new String[]{ "id_", "{\"test\":\"test\",\"key\":13,\"added\":52}" } );
        insertMany( DATA_0 );

        Result result = aggregate( $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkResultSet( result, expected );
    }


    @Test
    public void projectAddFieldsTest() {
        List<Object[]> expected = Arrays.asList(
                new String[]{ "id_", "52", "1" },
                new String[]{ "id_", "52", "1.3" },
                new String[]{ "id_", "52", "test" } );
        insertMany( DATA_0 );

        Result result = aggregate( $project( "{\"test\":1}" ), $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkResultSet( result, expected );
    }

    //$count


    @Test
    public void countTest() {
        List<Object[]> expected = ImmutableList.of(
                new String[]{ "3" } );
        insertMany( DATA_0 );

        Result result = aggregate( $count( "newName" ) );
        // todo test name

        MongoConnection.checkResultSet( result, expected );
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

    /*
    $addToSet
    $avg
    $count
    $first
    $last
    $max
    $mergeObjects
    $min
    $push
    $stdDevPop
    $stdDevSamp
    $sum
    */

    //$limit

    //$replaceRoot

    //$replaceWith

    //$set

    //$skip

    //$sort

    //$unset

    //$unwind


}
