/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.demo.DemoStore;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.stream.Stream;

@Slf4j
public class GraphStore extends DemoStore {

    private final static String[] files = new String[]{"/musicbrainz/artist.json"};
    private final TransactionManager transactionManager;

    public GraphStore( TransactionManager transactionManager, boolean local ) {
        super("demoneo4j", "cypher", DataModel.GRAPH, "neo4j" );
        this.transactionManager = transactionManager;
    }


    @Override
    public void setupNamespace( Statement statement ) {
    }

    private String buildBody(String query) throws JsonProcessingException {
        HTTPQuery httpQuery = new HTTPQuery( query, "cypher", this.name );
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString( httpQuery );
    }

    @Override
    public void loadData() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Stream<String> stream = getJarFileAsStream( "/graph/reduced_artists.json" );
            stream.forEach( line -> {
                String query = "";
                try {
                    Artist artist = objectMapper.readValue( line, Artist.class );
                    query = artist.query();
                    HttpClient client = HttpClient.newHttpClient();
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri( new URI( "http://localhost:13137/cypher" ) )
                            .header( "Content-Type", "application/json" )
                            .POST( BodyPublishers.ofString( this.buildBody( query ) ) )
                            .build();

                    HttpResponse<String> response = client.send( request, BodyHandlers.ofString() );
                } catch ( Exception e ) {
                    log.error( "Unable to add artist: {}. {}", query, e.getMessage() );
                }
            } );
        } catch ( Exception e ) {
            throw new RuntimeException(e);
        }
    }
}
