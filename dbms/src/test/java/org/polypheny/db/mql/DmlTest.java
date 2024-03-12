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
import static org.polypheny.db.TestHelper.MongoConnection.toDoc;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;


/**
 * Integration tests, which use the MongoQL-interface to observe
 * correctness of the MongoQL language and the document model
 */
@Tag("adapter")
public class DmlTest extends MqlTestTemplate {

    @AfterEach
    public void deleteAll() {
        deleteMany( "{}" );
    }


    @Test
    public void emptyTest() {
        String name = "test";
        execute( "db.createCollection(\"" + name + "\")" );

        DocResult result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of(), true, true ) );

    }


    @Test
    public void insertTest() {
        String data = "{\"test\":4}";
        insert( data );

        DocResult result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        ImmutableList.of( data ), true,
                        true ) );

    }


    @Test
    public void insertManyTest() {
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );

        DocResult result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        data,
                        true,
                        true ) );
    }


    @Test
    public void updateTest() {
        List<String> data = List.of( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );

        update( "{\"test\": 3}", "{\"$set\":{\"test\": 5}}" );

        DocResult result = find( "{}", "{}" );

        List<String> updated = List.of( "{\"test\":1}", "{\"test\":2}", "{\"test\":5}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        updated,
                        true,
                        true ) );
    }


    @Test
    public void updateIdTest() {
        List<Object> data = Arrays.asList( 1, 2, 3 );
        insertMany( data.stream().map( d -> toDoc( "test", d ) ).toList() );

        DocResult result = find( "{}", "{}" );

        BsonDocument doc = BsonDocument.parse( result.getData()[0] );

        BsonString id = doc.getString( "_id" );
        int content = doc.get( "test" ).asInt32().getValue();

        update( String.format( "{\"_id\": \"%s\"}", id.getValue() ), "{\"$set\":{\"test\": 5}}" );

        result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        data.stream()
                                .map( d -> {
                                    if ( d.equals( content ) ) {
                                        return 5;
                                    }
                                    return d;
                                } )
                                .map( d -> toDoc( "test", d ) )
                                .toList(), true, true ) );
    }


    @Test
    public void deleteSpecificTest() {
        List<String> data = List.of( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );
        DocResult result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        data,
                        true,
                        true ) );

        deleteMany( "{\"test\":2}" );
        data = List.of( "{\"test\":1}", "{\"test\":3}" );

        result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        data,
                        true,
                        true ) );


    }


    @Test
    public void deleteAllTest() {
        List<String> data = List.of( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );
        DocResult result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        data,
                        true,
                        true ) );

        deleteMany( "{}" );

        result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        result,
                        List.of(),
                        true,
                        true ) );


    }

}
