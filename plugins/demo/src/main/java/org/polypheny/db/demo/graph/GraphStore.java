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
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class GraphStore extends DemoStore {
    // All the local files path (in the resource directory) together with the node object they represent in the graph DB
    private final static FileNodePair ARTISTS = new FileNodePair( "/graph/artists.jsonl", "Artist" );
    private final static FileNodePair LABELS = new FileNodePair( "/graph/labels.jsonl", "Label" );
    private final static FileNodePair PLACES = new FileNodePair( "/graph/places.jsonl", "Place" );
    private final static FileNodePair AREAS = new FileNodePair( "/graph/areas.jsonl", "Area" );
    private final static FileNodePair RELEASES = new FileNodePair( "/graph/releases.jsonl", "Release" );
    private final static FileNodePair RELEASES_GROUP = new FileNodePair( "/graph/releases_group.jsonl", "ReleaseGroup" );
    private final static FileNodePair[] local_files = new FileNodePair[]{ARTISTS, LABELS, PLACES, AREAS, RELEASES, RELEASES_GROUP};

    // All relationship pairs between the different nodes in the graph DB
    private final static RelationshipPair ARTIST_RELEASE = new RelationshipPair( "Artist", "Release", "has_release");
    private final static RelationshipPair ARTIST_BEGIN_AREA = new RelationshipPair( "Artist", "Area", "began_in");
    private final static RelationshipPair ARTIST_END_AREA = new RelationshipPair( "Artist", "Area", "ended_in");
    private final static RelationshipPair LABEL_AREA = new RelationshipPair( "Label", "Area", "in_area");
    private final static RelationshipPair PLACE_AREA = new RelationshipPair( "Place", "Area", "in_area");
    private final static RelationshipPair[] relationship_pairs = new RelationshipPair[]{ARTIST_RELEASE, ARTIST_BEGIN_AREA, ARTIST_END_AREA, LABEL_AREA, PLACE_AREA};

    private final FileNodePair[] files;
    private final Function<String, Stream<String>> fileLoader;

    private final TransactionManager transactionManager;

    public GraphStore( TransactionManager transactionManager, boolean local ) {
        super("demoneo4j", "cypher", DataModel.GRAPH, "neo4j" );
        this.transactionManager = transactionManager;

        if (local) {
            this.files = local_files;
            this.fileLoader = this::getJarFileAsStream;
        }
        else {
            this.files = new FileNodePair[]{};
            this.fileLoader = this::getLocalFileAsStream;
        }
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
        // Build all nodes
        for (FileNodePair pair: this.files) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                Stream<String> stream = this.fileLoader.apply( pair.path() );
                stream.forEach( line -> {
                    try {
                        String query = String.format( "CREATE (n:%s {%s})", pair.node(), line);
                        System.out.println(query);
                        System.exit( 1 );

                        HttpClient client = HttpClient.newHttpClient();
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri( new URI( "http://localhost:13137/cypher" ) )
                                .header( "Content-Type", "application/json" )
                                .POST( BodyPublishers.ofString( this.buildBody( query ) ) )
                                .build();

                        HttpResponse<String> response = client.send( request, BodyHandlers.ofString() );
                    } catch ( Exception e ) {
                        log.error( "Unable to add {}: {}. {}", pair.node(), "", e.getMessage() );
                    }
                } );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
        }

        for (RelationshipPair pair: relationship_pairs) {
            String query = String.format( "MATCH (a:%s), (b:%s) WHERE a.id = b.id CREATE (a)->[:%s]->(t)", pair.from(), pair.to(), pair.name() );
        }
    }
}
