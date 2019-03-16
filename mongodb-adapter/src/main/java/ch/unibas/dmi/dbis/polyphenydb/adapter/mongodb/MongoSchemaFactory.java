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


import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Factory that creates a {@link MongoSchema}.
 *
 * Allows a custom schema to be included in a model.json file.
 */
public class MongoSchemaFactory implements SchemaFactory {

    // public constructor, per factory contract
    public MongoSchemaFactory() {
    }


    public Schema create( SchemaPlus parentSchema, String name, Map<String, Object> operand ) {
        final String host = (String) operand.get( "host" );
        final String database = (String) operand.get( "database" );
        final String authMechanismName = (String) operand.get( "authMechanism" );

        final MongoClientOptions.Builder options = MongoClientOptions.builder();

        final List<MongoCredential> credentials = new ArrayList<>();
        if ( authMechanismName != null ) {
            final MongoCredential credential = createCredentials( operand );
            credentials.add( credential );
        }

        return new MongoSchema( host, database, credentials, options.build() );
    }


    private MongoCredential createCredentials( Map<String, Object> map ) {
        final String authMechanismName = (String) map.get( "authMechanism" );
        final AuthenticationMechanism authenticationMechanism = AuthenticationMechanism.fromMechanismName( authMechanismName );
        final String username = (String) map.get( "username" );
        final String authDatabase = (String) map.get( "authDatabase" );
        final String password = (String) map.get( "password" );

        switch ( authenticationMechanism ) {
            case PLAIN:
                return MongoCredential.createPlainCredential( username, authDatabase, password.toCharArray() );
            case SCRAM_SHA_1:
                return MongoCredential.createScramSha1Credential( username, authDatabase, password.toCharArray() );
            case GSSAPI:
                return MongoCredential.createGSSAPICredential( username );
            case MONGODB_CR:
                return MongoCredential.createMongoCRCredential( username, authDatabase, password.toCharArray() );
            case MONGODB_X509:
                return MongoCredential.createMongoX509Credential( username );
        }
        throw new IllegalArgumentException( "Unsupported authentication mechanism " + authMechanismName );
    }
}

