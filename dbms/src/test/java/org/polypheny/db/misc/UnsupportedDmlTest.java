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

package org.polypheny.db.misc;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
public class UnsupportedDmlTest extends MqlTestTemplate {

    @Test
    public void dmlEnumerableTest() {
        insert( "{\"hi\":3,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": 2}})" );
        DocResult res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        res,
                        ImmutableList.of( "{\"hi\":3,\"stock\":5}" ),
                        true,
                        true ) );
    }


    @Test
    public void dmlEnumerableFilterTest() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": 3}})" );
        DocResult res = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet(
                        res,
                        ImmutableList.of(
                                "{\"hi\":3,\"stock\":6}",
                                "{\"hi\":5,\"stock\":3}"
                        ),
                        true,
                        true ) );
    }


    @Test
    public void dmlEnumerableTestFilter() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": -3}})" );
        DocResult res = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkDocResultSet( res,
                        ImmutableList.of(
                                "{\"hi\":3,\"stock\":0}",
                                "{\"hi\":3,\"stock\":29}",
                                "{\"hi\":5,\"stock\":3}"
                        ),
                        true,
                        true ) );

    }


    @Test
    public void dmlEnumerableTestDeleteOne() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.deleteOne({ \"hi\": 3 })" );
        DocResult res = find( "{}", "{}" );

        assertEquals( 2, res.getData().length );

    }


    @Test
    public void dmlEnumerableTestDeleteMany() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.deleteMany({ \"hi\": 3 })" );
        DocResult res = find( "{}", "{}" );

        assertEquals( 1, res.getData().length );

    }


}
