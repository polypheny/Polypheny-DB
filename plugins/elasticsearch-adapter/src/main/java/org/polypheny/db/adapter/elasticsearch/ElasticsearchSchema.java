/*
 * Copyright 2019-2023 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.adapter.elasticsearch;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.Namespace.Schema;
import org.polypheny.db.schema.impl.AbstractNamespace;


/**
 * Schema mapped onto an index of ELASTICSEARCH types.
 *
 * Each table in the schema is an ELASTICSEARCH type in that index.
 */
public class ElasticsearchSchema extends AbstractNamespace implements Schema {

    private final String index;

    private final RestClient client;

    private final ObjectMapper mapper;

    private final Map<String, Entity> tableMap;

    /**
     * Default batch size to be used during scrolling.
     */
    private final int fetchSize;


    /**
     * Allows schema to be instantiated from existing elastic search client. This constructor is used in tests.
     *
     * @param client existing client instance
     * @param mapper mapper for JSON (de)serialization
     * @param index name of ES index
     */
    public ElasticsearchSchema( long id, RestClient client, ObjectMapper mapper, String index ) {
        this( id, client, mapper, index, null );
    }


    public ElasticsearchSchema( long id, RestClient client, ObjectMapper mapper, String index, String type ) {
        this( id, client, mapper, index, type, ElasticsearchTransport.DEFAULT_FETCH_SIZE );
    }


    @VisibleForTesting
    ElasticsearchSchema( long id, RestClient client, ObjectMapper mapper, String index, String type, int fetchSize ) {
        super( id );
        this.client = Objects.requireNonNull( client, "client" );
        this.mapper = Objects.requireNonNull( mapper, "mapper" );
        this.index = Objects.requireNonNull( index, "index" );
        Preconditions.checkArgument( fetchSize > 0, "invalid fetch size. Expected %s > 0", fetchSize );
        this.fetchSize = fetchSize;
        if ( type == null ) {
            try {
                this.tableMap = createTables( listTypesFromElastic() );
            } catch ( IOException e ) {
                throw new UncheckedIOException( "Couldn't get types for " + index, e );
            }
        } else {
            this.tableMap = createTables( Collections.singleton( type ) );
        }

    }


    @Override
    protected Map<String, Entity> getTableMap() {
        return tableMap;
    }


    private Map<String, Entity> createTables( Iterable<String> types ) {
        final ImmutableMap.Builder<String, Entity> builder = ImmutableMap.builder();
        for ( String type : types ) {
            final ElasticsearchTransport transport = new ElasticsearchTransport( client, mapper, index, type, fetchSize );
            builder.put( type, new ElasticsearchEntity( transport ) );
        }
        return builder.build();
    }


    /**
     * Queries {@code _mapping} definition to automatically detect all types for an index
     *
     * @return Set of types associated with this index
     * @throws IOException for any IO related issues
     * @throws IllegalStateException if reply is not understood
     */
    private Set<String> listTypesFromElastic() throws IOException {
        final String endpoint = "/" + index + "/_mapping";
        final Response response = client.performRequest( "GET", endpoint );
        try ( InputStream is = response.getEntity().getContent() ) {
            final JsonNode root = mapper.readTree( is );
            if ( !root.isObject() || root.size() != 1 ) {
                final String message = String.format( Locale.ROOT, "Invalid response for %s/%s. Expected object of size 1 got %s (of size %d)", response.getHost(), response.getRequestLine(), root.getNodeType(), root.size() );
                throw new IllegalStateException( message );
            }

            JsonNode mappings = root.iterator().next().get( "mappings" );
            if ( mappings == null || mappings.size() == 0 ) {
                final String message = String.format( Locale.ROOT, "Index %s does not have any types", index );
                throw new IllegalStateException( message );
            }

            Set<String> types = Sets.newHashSet( mappings.fieldNames() );
            types.remove( "_default_" );
            return types;
        }
    }


    public String getIndex() {
        return index;
    }

}

