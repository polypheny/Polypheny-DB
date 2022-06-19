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

package org.polypheny.db.misc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.webui.models.Result;

@Category({ AdapterTestSuite.class, CassandraExcluded.class, FileExcluded.class }) // todo fix error with filter in file
public class UnsupportedDmlTest extends MqlTestTemplate {

    @Test
    public void dmlEnumerableTest() {
        insert( "{\"hi\":3,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": 2}})" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        res,
                        ImmutableList.of( new String[]{ "{\"hi\":3,\"stock\":5}" } ), true ) );
    }


    @Test
    public void dmlEnumerableFilterTest() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": 3}})" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkUnorderedResultSet(
                        res,
                        ImmutableList.of(
                                new String[]{ "{\"hi\":3,\"stock\":6}" },
                                new String[]{ "{\"hi\":5,\"stock\":3}" }
                        ), true ) );
    }


    @Test
    public void dmlEnumerableTestFilter() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.update({ \"hi\": 3 },{\"$inc\": {\"stock\": -3}})" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertTrue(
                MongoConnection.checkUnorderedResultSet( res,
                        ImmutableList.of(
                                new String[]{ "{\"hi\":3,\"stock\":0}" },
                                new String[]{ "{\"hi\":3,\"stock\":29}" },
                                new String[]{ "{\"hi\":5,\"stock\":3}" }
                        ), true ) );

    }


    @Test
    public void dmlEnumerableTestDeleteOne() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.deleteOne({ \"hi\": 3 })" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertEquals( 2, res.getData().length );

    }


    @Test
    public void dmlEnumerableTestDeleteMany() {
        insert( "{\"hi\":3,\"stock\":3}" );
        insert( "{\"hi\":3,\"stock\":32}" );
        insert( "{\"hi\":5,\"stock\":3}" );

        MongoConnection.executeGetResponse( "db.test.deleteMany({ \"hi\": 3 })" );
        Result res = find( "{}", "{}" );
        System.out.println( Arrays.deepToString( res.getData() ) );

        assertEquals( 1, res.getData().length );

    }


}
