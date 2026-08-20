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

package org.polypheny.db.demo;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.multimodel.PolyStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class DocumentStore extends DemoStore {
    private final String[] files;
    private final static String[] localFiles = new String[]{ "/musicbrainz/artists.json" };
    private final static String[] remoteFiles = new String[]{};
    private final Function<String, Stream<String>> fileLoader;

    private final TransactionManager transactionManager;


    public DocumentStore( TransactionManager transactionManager, boolean local ) {
        super( "demomongodb", "mongo", DataModel.DOCUMENT, "mongodb" );
        this.transactionManager = transactionManager;

        if (local) {
            this.files = localFiles;
            this.fileLoader = this::getJarFileAsStream;
        }
        else {
            this.files = remoteFiles;
            this.fileLoader = this::getLocalFileAsStream;
        }
    }

    @Override
    public void setupNamespace( Statement statement ) {
        DdlManager ddlManager = DdlManager.getInstance();

        if ( this.dataStore.isPresent() ) {
            List<DataStore<?>> stores = List.of( this.dataStore.get() );
            ddlManager.createCollection( this.namespaceId, "artist", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection( this.namespaceId, "recordings", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection( this.namespaceId, "masters", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection( this.namespaceId, "releases", true, stores, PlacementType.AUTOMATIC, statement );
        } else {
            log.warn( "No datastore present in {}. Unable to create collections", this.name );
        }
    }


    @Override
    public void loadData() {
        try {

            Transaction transaction = this.transactionManager.startTransaction( Catalog.defaultUserId, this.namespaceId, new QueryAnalyzer(), ORIGIN );

            log.info( "Loading document data" );
            PolyStatement polyStatement = this.getPolyConnection().get().createPolyStatement();

            for (String file_path: this.files) {
                Stream<String> lines = fileLoader.apply( file_path );
                lines.forEach( line -> {
                    try {
                        line = line.replace( "'", "" );
                        line = line.replace( "\n", "" );
                        line = line.replace( "\t", "" );
                        String query =  String.format( "db.artist.insertOne(%s)", line );
                        System.out.println(query);
                        try {
                            new com.fasterxml.jackson.databind.ObjectMapper().readTree(line);
                        } catch (Exception e) {
                            System.out.println("Parse error: " + e.getMessage());
                        }
                        polyStatement.execute( this.name, "mongo", query );
                    } catch ( PrismInterfaceServiceException e ) {
                        log.error( e.getMessage() );
                    }
                } );
                return;
            }
        }
        catch ( SQLException e ) {
            log.error( e.getMessage() );
        }
    }
}
