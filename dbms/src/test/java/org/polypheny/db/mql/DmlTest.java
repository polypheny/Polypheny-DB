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
                        ImmutableList.of( new Object[]{ "id_", data } ) ) );

    }


    @Test
    @Category(FileExcluded.class)
    public void insertManyTest() {
        List<String> data = Arrays.asList( "{\"test\":1}", "{\"test\":2}", "{\"test\":3}" );
        insertMany( data );

        Result result = find( "{}", "{}" );

        assertTrue(
                MongoConnection.checkResultSet(
                        result,
                        data.stream()
                                .map( d -> new Object[]{ "id_", d } )
                                .collect( Collectors.toList() ) ) );
    }

}
