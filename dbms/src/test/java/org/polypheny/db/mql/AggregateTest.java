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

}
