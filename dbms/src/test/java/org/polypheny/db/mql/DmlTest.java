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
import java.util.List;
import kong.unirest.HttpResponse;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Test;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.mongoql.model.Result;

public class DmlTest extends MqlTestTemplate {

    @Test
    public void insert() {
        MongoConnection connection = new MongoConnection();

        connection.execute( "db.test.insert({\"test\":3})" );

        HttpResponse<String> res = connection.execute( "db.test.find({})" );
        List<Result> results = connection.getBody( res );
        assertTrue(
                MongoConnection.checkResultSet( results,
                        ImmutableList.of( new Object[]{ "test", new BsonDocument( "test", new BsonInt32( 3 ) ).toJson().replace( " ", "" ) } ) ) );
    }

}
