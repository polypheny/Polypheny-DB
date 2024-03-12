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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
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

    private final List<String> DATA_3_5 = Arrays.asList(
            "{\"val\":3,\"test\":[3,4.1]}",
            "{\"val\":\"31\",\"test\":[\"test\",null,{ \"test\": 25 }]}",
            "{\"test\":13}" );


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
                        new Object[]{ 1, 1 }, // field key is not present, which is unset, not null
                        new Object[]{ 1.3, 1.3, "{\"key\":\"val\"}" },
                        new Object[]{ "test", "test", 13 } ),
                "newName1", "test", "newName2" );
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
        List<String> expected = ImmutableList.of(
                document( kv( string( "key" ), document( kv( string( "key" ), string( "val" ) ) ) ), kv( "test", 1.3 ) )
        );
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
                        new Object[]{ 52, 1 },
                        new Object[]{ 52, 1.3 },
                        new Object[]{ 52, "test" } ),
                "added", "test" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $project( "{\"test\":1}" ), $addFields( "{\"added\": 52}" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$count


    @Test
    public void countTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                        new Object[]{ 3 } ),
                "newName" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $count( "newName" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
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
                        new Object[]{ 1 },
                        new Object[]{ "test" },
                        new Object[]{ 1.3 } ),
                "_id" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $group( "{\"_id\":\"$test\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );

        expected = MongoConnection.arrayToDoc( ImmutableList.of(
                        new Object[]{ null },
                        new Object[]{ 13 },
                        new Object[]{ document( kv( string( "key" ), string( "val" ) ) ) } ),
                "_id" );

        result = aggregate( $group( "{\"_id\":\"$key\"}" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    @Test
    public void groupSubFieldTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                        new Object[]{ null },
                        new Object[]{ "val" } ),
                "_id" );
        insertMany( DATA_0 );

        DocResult result = aggregate( $group( document( kv( string( "_id" ), string( "$key.key" ) ) ) ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    @Test
    public void groupAvgTest() {
        List<String> expected = MongoConnection.arrayToDoc( List.of(
                        new Object[]{ "val2", 5.0 },
                        new Object[]{ "val1", 7.0 } ),
                "_id", "avgValue" );
        insertMany( DATA_1 );

        DocResult result = aggregate( $group(
                document( kv( string( "_id" ), string( "$test" ) ), kv( string( "avgValue" ), document( kv( string( "$avg" ), string( "$key" ) ) ) ) ) ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    //$limit
    @Test
    public void limitTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":\"val1\",\"key\":1}" );
        insertMany( DATA_1 );

        DocResult result = aggregate( $sort( document( kv( string( "key" ), 1 ) ) ), $limit( 1 ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

        result = aggregate( $sort( document( kv( string( "key" ), 1 ) ) ), $limit( 2 ) );

        expected = ImmutableList.of(
                "{\"test\":\"val1\",\"key\":1}",
                "{\"test\":\"val2\",\"key\":5}" );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$replaceRoot


    @Test
    public void replaceRootTest() {
        // only doc allowed
        List<String> expected = Arrays.asList(
                document( kv( string( "key" ), 1 ) ),
                document( kv( string( "key1" ), 5 ) ),
                document( kv( string( "key" ), 13 ) ) );

        insertMany( DATA_2 );

        DocResult result = aggregate( $replaceRoot( "$test" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    @Test
    public void replaceRootNotDocTest() {
        try {
            insertMany( DATA_2 );

            DocResult result = aggregate( $replaceRoot( "$test.key" ) );
            Assertions.fail();
        } catch ( Exception e ) {
            // empty on purpose
        }
    }

    //$replaceWith


    @Test
    public void replaceWithTest() {
        List<String> expected = Arrays.asList(
                document( kv( string( "key" ), 1 ) ),
                document( kv( string( "key1" ), 5 ) ),
                document( kv( string( "key" ), 13 ) ) );

        insertMany( DATA_2 );

        DocResult result = aggregate( $replaceWith( "$test" ) );

        MongoConnection.checkDocResultSet( result, expected, false, true );
    }


    @Test
    public void replaceWithNonDocTest() {

        insertMany( DATA_2 );

        try {
            DocResult result = aggregate( $replaceWith( "$test.key" ) );

            // this has to fail
            Assertions.fail();
        } catch ( Exception e ) {
            // empty on purpose
        }
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
    public void skipTest() {
        List<String> expected = ImmutableList.of(
                "{\"test\":1.3,\"key\":{\"key\":\"val\"}}",
                "{\"test\":\"test\",\"key\":13}" );

        insertMany( DATA_0 );

        // we sort to assure correct order
        DocResult result = aggregate( $sort( document( kv( string( "key" ), 1 ) ) ), $skip( 1 ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

        expected = ImmutableList.of(
                "{\"test\":\"test\",\"key\":13}" );

        result = aggregate( $sort( document( kv( string( "key" ), 1 ) ) ), $skip( 2 ) );

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
                document(),
                document(),
                document() );

        result = aggregate( $unset( Arrays.asList( string( "key" ), string( "test" ) ) ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );
    }

    //$unwind


    @Test
    public void unwindTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                        new Object[]{ 3 },
                        new Object[]{ 2 },
                        new Object[]{ "test" },
                        new Object[]{ 3 },
                        new Object[]{ 51 },
                        new Object[]{ 13 } ),
                "test" );

        insertMany( DATA_3 );

        DocResult result = aggregate( $project( "{\"test\":1}" ), $unwind( "$test" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

    }


    @Test
    public void unwindTypesTest() {
        List<String> expected = MongoConnection.arrayToDoc( ImmutableList.of(
                        new Object[]{ 3 },
                        new Object[]{ 4.1 },
                        new Object[]{ "test" },
                        new Object[]{ null },
                        new Object[]{ document( kv( string( "test" ), 25 ) ) },
                        new Object[]{ 13 } ),
                "test" );

        insertMany( DATA_3_5 );

        DocResult result = aggregate( $project( "{\"test\":1}" ), $unwind( "$test" ) );

        MongoConnection.checkDocResultSet( result, expected, true, true );

    }

}
