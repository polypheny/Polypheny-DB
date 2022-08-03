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
import static org.polypheny.db.TestHelper.MongoConnection.toDoc;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.webui.models.Result;


/**
 * Integration tests, which use the MongoQL-interface to observe
 * correctness of the MongoQL language and the document model
 */
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class DmlTest extends MqlTestTemplate {


    @Test
    public void insertTest() {
        String data = "{\"test\":4}";
        insert( data );

        Result result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        ImmutableList.of( new Object[]{ data } ), true ) );

    }


    @Test
    @Category(FileExcluded.class)
    public void insertManyTest() {
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );

        Result result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        data.stream()
                                .map( d -> new String[]{ d } )
                                .collect( Collectors.toList() ), true ) );
    }


    @Test
    @Category(FileExcluded.class)
    public void updateTest() {
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );

        update( "{\"test\": 3}", "{\"$set\":{\"test\": 5}}" );

        Result result = find( "{}", "{}" );

        List<String> updated = Arrays.asList( "{\"test\":1}", "{\"test\":2}", "{\"test\":5}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        updated.stream()
                                .map( d -> new String[]{ d } )
                                .collect( Collectors.toList() ), true ) );
    }


    @Test
    @Category(FileExcluded.class)
    public void updateIdTest() {
        List<Object> data = Arrays.asList( 1, 2, 3 );
        insertMany( data.stream().map( d -> toDoc( "test", d ) ).collect( Collectors.toList() ) );

        Result result = find( "{}", "{}" );

        BsonDocument doc = BsonDocument.parse( result.getData()[0][0] );

        BsonString id = doc.getString( "_id" );
        int content = doc.get( "test" ).asInt32().getValue();

        update( String.format( "{\"_id\": \"%s\"}", id.getValue() ), "{\"$set\":{\"test\": 5}}" );

        result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        result,
                        data.stream()
                                .map( d -> {
                                    if ( d.equals( content ) ) {
                                        return 5;
                                    }
                                    return d;
                                } )
                                .map( d -> toDoc( "test", d ) )
                                .map( d -> new String[]{ d } )
                                .collect( Collectors.toList() ), true ) );
    }


    @Test
    @Category(FileExcluded.class)
    public void deleteTest() {
        deleteMany( "{}" );
    }

}
