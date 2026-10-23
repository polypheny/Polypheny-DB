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

package org.polypheny.db.demo.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.demo.DemoStore;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.multimodel.PolyStatement;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class DocumentStore extends DemoStore {
    public static class FileCollectionPair {
        public String path;
        public String collection;

        FileCollectionPair(String path, String collection ) {
            this.path = path;
            this.collection = collection;
        }
    }

    private final static FileCollectionPair ARTISTS = new FileCollectionPair( "artists.jsonl", "artist" );
    private final static FileCollectionPair LABELS = new FileCollectionPair( "labels.jsonl", "label" );
    private final static FileCollectionPair MASTERS = new FileCollectionPair( "masters.jsonl", "master" );
    private final static FileCollectionPair RELEASES = new FileCollectionPair( "releases.jsonl", "release" );

    private final FileCollectionPair[] files;
    private final FileCollectionPair[] local_files = new FileCollectionPair[]{ARTISTS, LABELS, MASTERS, RELEASES};
    private final Function<String, Stream<String>> fileLoader;

    private final TransactionManager transactionManager;


    public DocumentStore( TransactionManager transactionManager, boolean local ) {
        super( "demomongodb", "mongo", DataModel.DOCUMENT, "mongodb" );
        this.transactionManager = transactionManager;

        if (local) {
            this.files = local_files;
            this.fileLoader = this::getJarFileAsStream;
        }
        else {
            this.files = new FileCollectionPair[]{};
            this.fileLoader = this::getLocalFileAsStream;
        }
    }

    @Override
    public void setupNamespace( Statement statement ) {
        DdlManager ddlManager = DdlManager.getInstance();

        if ( this.dataStore.isPresent() ) {
            List<DataStore<?>> stores = List.of( this.dataStore.get() );
            for ( FileCollectionPair group: this.files) {
                ddlManager.createCollection( this.namespaceId, group.collection, true, stores, PlacementType.AUTOMATIC, statement );
            }
        } else {
            log.warn( "No datastore present in {}. Unable to create collections", this.name );
        }
    }

    // Helper method to build and execute the batched command
    private void executeBatch( PolyStatement statement, String collection, List<String> jsonNodes) {
        String jsonArrayContent = String.join(",", jsonNodes);
        String query = String.format("db.%s.insertMany([%s])", collection, jsonArrayContent);
        try {
            statement.execute(this.name, "mongo", query);
        } catch (PrismInterfaceServiceException e) {
            log.error( "Exception for batch query: {}. {}", query, e.getMessage() );
            throw new RuntimeException( e );
        } catch ( Exception e ) {
            log.error( e.getMessage() );
            throw new RuntimeException( e );
        }
    }

    @Override
    public void loadData() {
        log.info( "Loading document data" );

        ObjectMapper objectMapper = new ObjectMapper();
        int batchSize = 1000; // Adjust batch size based on memory/database payload limits

        PolyStatement polyStatement;
        try {
            polyStatement = this.getPolyConnection().get().createPolyStatement();
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
        for ( FileCollectionPair group : this.files) {
            String file_path = group.path;
            String collection = group.collection;
            try (InputStream is = Files.newInputStream(Path.of(file_path))) {

                try (MappingIterator<JsonNode> it = objectMapper.readerFor(JsonNode.class).readValues(is)) {

                    List<String> batchBuffer = new ArrayList<>(batchSize);

                    while (it.hasNext()) {
                        JsonNode node = it.next();
                        // Clean newline characters from individual JSON objects
                        batchBuffer.add(node.toString().replace("\n", "").replace("\r", ""));

                        if (batchBuffer.size() >= batchSize) {
                            executeBatch( polyStatement, collection, batchBuffer);
                            batchBuffer.clear();
                        }
                    }

                    // Flush remaining documents in the batch buffer
                    if (!batchBuffer.isEmpty()) {
                        executeBatch( polyStatement, collection, batchBuffer);
                    }

                } catch (Exception e) {
                    throw new RuntimeException("Error processing file iterator: " + file_path, e);
                }

            } catch (Exception e) {
                throw new RuntimeException("Error reading file: " + file_path, e);
            }
        }
    }
}
