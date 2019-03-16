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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;

import org.apache.http.HttpHost;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Factory that creates an {@link ElasticsearchSchema}.
 *
 * Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class ElasticsearchSchemaFactory implements SchemaFactory {

    public ElasticsearchSchemaFactory() {
    }


    @Override
    public Schema create( SchemaPlus parentSchema, String name, Map<String, Object> operand ) {

        final Map map = (Map) operand;

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure( JsonParser.Feature.ALLOW_SINGLE_QUOTES, true );

        try {
            final String coordinatesString = (String) map.get( "coordinates" );
            Preconditions.checkState( coordinatesString != null, "'coordinates' is missing in configuration" );

            final Map<String, Integer> coordinates = mapper.readValue( coordinatesString,
                    new TypeReference<Map<String, Integer>>() {
                    } );

            // create client
            final RestClient client = connect( coordinates );

            final String index = (String) map.get( "index" );
            Preconditions.checkState( index != null, "'index' is missing in configuration" );
            return new ElasticsearchSchema( client, new ObjectMapper(), index );
        } catch ( IOException e ) {
            throw new RuntimeException( "Cannot parse values from json", e );
        }
    }


    /**
     * Builds elastic rest client from user configuration
     *
     * @param coordinates list of {@code hostname/port} to connect to
     * @return newly initialized low-level rest http client for ES
     */
    private static RestClient connect( Map<String, Integer> coordinates ) {
        Objects.requireNonNull( coordinates, "coordinates" );
        Preconditions.checkArgument( !coordinates.isEmpty(), "no ES coordinates specified" );
        final Set<HttpHost> set = new LinkedHashSet<>();
        for ( Map.Entry<String, Integer> entry : coordinates.entrySet() ) {
            set.add( new HttpHost( entry.getKey(), entry.getValue() ) );
        }

        return RestClient.builder( set.toArray( new HttpHost[0] ) ).build();
    }

}

