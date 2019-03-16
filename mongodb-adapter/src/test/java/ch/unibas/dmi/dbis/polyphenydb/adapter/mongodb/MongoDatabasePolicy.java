/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.mongodb;


import ch.unibas.dmi.dbis.polyphenydb.test.MongoAssertions;

import com.github.fakemongo.Fongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.junit.rules.ExternalResource;

import java.util.Objects;


/**
 * Instantiates a new connection to Fongo (or Mongo) database depending on the current profile (unit or integration tests).
 *
 * By default, this rule is executed as part of a unit test and in-memory database <a href="https://github.com/fakemongo/fongo">Fongo</a> is used.
 *
 * However, if the maven profile is set to {@code IT} (eg. via command line {@code $ mvn -Pit install}) this rule will connect to an existing (external) Mongo instance ({@code localhost}).
 */
class MongoDatabasePolicy extends ExternalResource {

    private static final String DB_NAME = "test";

    private final MongoDatabase database;
    private final MongoClient client;


    private MongoDatabasePolicy( MongoClient client ) {
        this.client = Objects.requireNonNull( client, "client" );
        this.database = client.getDatabase( DB_NAME );
    }


    /**
     * Creates an instance based on current maven profile (as defined by {@code -Pit}).
     *
     * @return new instance of the policy to be used by unit tests
     */
    static MongoDatabasePolicy create() {
        final MongoClient client;
        if ( MongoAssertions.useMongo() ) {
            // use to real client (connects to mongo)
            client = new MongoClient();
        } else if ( MongoAssertions.useFongo() ) {
            // in-memory DB (fake Mongo)
            client = new Fongo( MongoDatabasePolicy.class.getSimpleName() ).getMongo();
        } else {
            throw new UnsupportedOperationException( "I can only connect to Mongo or Fongo instances" );
        }

        return new MongoDatabasePolicy( client );
    }


    MongoDatabase database() {
        return database;
    }


    @Override
    protected void after() {
        client.close();
    }

}
