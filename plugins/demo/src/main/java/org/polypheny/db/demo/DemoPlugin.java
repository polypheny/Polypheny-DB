/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.adapter.mongodb.MongoPlugin.MongoStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.demo.models.document.Artist;
import org.polypheny.db.docker.AutoDocker;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.models.AutoDockerStatus;
import org.polypheny.db.docker.models.DockerInstanceInfo;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statements.PIPreparedNamedStatement;
import org.polypheny.db.prisminterface.statements.PIUnparameterizedStatement;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.webui.crud.LanguageCrud;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;


@Slf4j
public class DemoPlugin extends PolyPlugin {
    DockerInstance dockerInstance;

    private final static String ORIGIN = "Demo";

    private final static String MONGODB = "demomongodb";
    private long mongodbNampespaceId;

    private final static String NEO4J = "demoneo4j";
    private long neo4jNampespaceId;

    private final static String POSTGRES = "demopostgres";
    private long postgresNampespaceId;

    private final static String NOTEBOOK = "demo.ipynb";
    private final static String RELATIONAL_DATA = "";
    private final static String GRAPH_DATA = "";
    private final static String DOCUMENT_DATA = "/recording.json";

    private TransactionManager transactionManager;

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public DemoPlugin( PluginContext context ) {
        super( context );
    }

    @Override
    public void afterRestoreInit() {
        Transaction transaction = this.transactionManager.startTransaction( Catalog.defaultUserId, new QueryAnalyzer(), "Demo" );
        createStores();
        createNamespaces( transaction );
        createNotebook();

        // Data insertion
        loadDocumentData( this.transactionManager );
    }

    @Override
    public void afterTransactionInit( TransactionManager manager ) {
        this.transactionManager = manager;
    }

    @Override
    public void afterCatalogInit() {
        super.afterCatalogInit();

        log.info( "Auto connecting to docker instance" );
        AutoDocker.getInstance().doAutoConnect();

        AutoDockerStatus status = AutoDocker.getInstance().getStatus();
        if (!status.available()) {
            log.error( "Auto docker instance not available" );
        }
        else if (!status.connected()) {
            log.error( "Auto docker instance not connected" );
        }
        else {
            log.info( "Successfully connected to docker instance" );
        }

        Optional<DockerInstance> dockerInstanceOptional = Optional.empty();
        for ( DockerInstanceInfo dockerInstanceInfo: DockerManager.getInstance().getDockerInstancesMap()) {
            if ( Objects.equals( dockerInstanceInfo.host().hostname(), "localhost" ) ) {
                dockerInstanceOptional = DockerManager.getInstance().getInstanceById( dockerInstanceInfo.id() );
            }
        }

        if (dockerInstanceOptional.isPresent()) {
            this.dockerInstance = dockerInstanceOptional.get();
        }
        else {
            log.error( "Unable to find localhost docker instance" );
        }

    }


    @Override
    public void stop() {
    }

    public void createNamespaces(Transaction transaction) {
        log.info( "Creating namespaces" );
        Statement statement = transaction.createStatement();

        DdlManager ddlManager = DdlManager.getInstance();
        this.mongodbNampespaceId = ddlManager.createNamespace( MONGODB, DataModel.DOCUMENT, true, true, false,  statement);
        this.neo4jNampespaceId = ddlManager.createNamespace( NEO4J, DataModel.GRAPH, true, true, false,  statement);
        this.postgresNampespaceId = ddlManager.createNamespace( POSTGRES, DataModel.RELATIONAL, true, true, false,  statement);

        Optional<DataStore<?>> mongodbStore = AdapterManager.getInstance().getStore( MONGODB );
        if (mongodbStore.isPresent()) {
            List<DataStore<?>> stores = List.of( mongodbStore.get() );

            ddlManager.createCollection(this.mongodbNampespaceId, "artist", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection(this.mongodbNampespaceId, "recordings", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection(this.mongodbNampespaceId, "masters", true, stores, PlacementType.AUTOMATIC, statement );
            ddlManager.createCollection(this.mongodbNampespaceId, "releases", true, stores, PlacementType.AUTOMATIC, statement );
        }
        else {
            log.error( "MongoDB store not present" );
        }

    }

    public void createStores( ) {
        log.info( "Creating demo stores" );

        DdlManager ddlManager = DdlManager.getInstance();

        log.info( "Creating demo MongoDB store" );
        AdapterTemplate mongodbTemplate = AdapterTemplate.fromString( "mongodb", AdapterType.STORE );
        Map<String, String> mongodbSettings = mongodbTemplate.getDefaultSettings();
        mongodbSettings.put( "instanceId", String.valueOf(this.dockerInstance.getInfo().id()) );
        ddlManager.createStore( "demomongodb", mongodbTemplate.getAdapterName(), AdapterType.STORE, mongodbSettings, DeployMode.DOCKER);

        log.info( "Creating demo Neo4j store" );
        AdapterTemplate neo4jTemplate = AdapterTemplate.fromString( "neo4j", AdapterType.STORE );
        Map<String, String> neo4jSettings = neo4jTemplate.getDefaultSettings();
        neo4jSettings.put( "instanceId", String.valueOf(this.dockerInstance.getInfo().id()) );
        ddlManager.createStore( "demoneo4j", neo4jTemplate.getAdapterName(), AdapterType.STORE, neo4jSettings, DeployMode.DOCKER);

        log.info( "Creating demo PostgreSQL store" );
        AdapterTemplate postgresTemplate = AdapterTemplate.fromString( "postgresql", AdapterType.STORE );
        Map<String, String> postgresSettings = postgresTemplate.getDefaultSettings();
        postgresSettings.put( "instanceId", String.valueOf(this.dockerInstance.getInfo().id()) );
        ddlManager.createStore( "demopostgres", postgresTemplate.getAdapterName(), AdapterType.STORE, postgresSettings, DeployMode.DOCKER);

        log.info( "Successfully created demo stores" );
    }

    public void createNotebook() {
        File notebookFile = PolyphenyHomeDirManager.getInstance().registerNewFile( String.format( "data/jupyter/notebooks/%s", NOTEBOOK ) );
        try (InputStream inputStream = DemoPlugin.class.getResourceAsStream( String.format( "/%s", NOTEBOOK ) ) ) {
            if (inputStream == null) {
                throw new FileNotFoundException( String.format( "Unable to find file /%s", NOTEBOOK ) );
            }
            Files.copy(inputStream, notebookFile.toPath(), StandardCopyOption.REPLACE_EXISTING );
        }
        catch ( IOException E ) {
            log.error( E.getMessage() );
        }
    }

    public void loadRelationalData() {
    }

    public void loadGraphData() {
    }

    public void MQLQuery(Transaction transaction, String query, long namespaceId) {
        QueryContext queryContext = QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( "MQL" ) )
                .namespaceId( namespaceId )
                .origin( ORIGIN )
                .isAnalysed( true )
                .build();

        queryContext.addTransaction( transaction );

        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( queryContext );
        for (ExecutedContext executedContext: executedContexts) {
            log.info(executedContext.toString());

            if (executedContext.getException().isPresent()) {
                log.warn( "Caught exception", executedContext.getException().get() );
            }
        }
    }

    public void loadDocumentData(TransactionManager transactionManager) {
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, this.mongodbNampespaceId, new QueryAnalyzer(), ORIGIN );

        log.info( "Loading document data" );
        Stream<String> lines = getFileAsStream( "/home/mathieu/Documents/Unibasel/Hiwi/PolyphenyDemo/discogs/discogs_artists.json", false );
        LogicalUser defaultUser = Catalog.getInstance().getUsers().get( Catalog.defaultUserId );
        //PIClient piClient = new PIClient(Catalog.defaultUserId, defaultUser, transactionManager, Catalog.getInstance().getLogicalDoc( this.mongodbNampespaceId ).getLogicalNamespace(), )
        //PIPreparedNamedStatement statement = new PIPreparedNamedStatement(  )

        lines.findFirst().ifPresent( line -> {
            log.info( "Inserting new line" );
            MQLQuery( transaction, String.format( "db.artist.insert(%s)", line ), this.mongodbNampespaceId );
        });


        //lines.forEach( line -> System.out.printf("Line: %s\n", line ));
        /*
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false );
        lines.forEach( line -> {
            try {
                Artist artist = objectMapper.readValue( line, Artist.class );
                System.out.println(artist.name);
            } catch ( JsonProcessingException e ) {
                throw new RuntimeException( e );
            }
            System.exit( 1 );
        } );
        */
    }

    public Stream<String> getFileAsStream(String path, boolean classpath) {
        if (path.isEmpty()) {
            log.error( "No path specified" );
            return Stream.empty();
        }

        log.debug( "Loading data from resource path {}", path );

        try {
            InputStream inputStream;
            if ( classpath ) {
                inputStream = DemoPlugin.class.getResourceAsStream( path );
            } else {
                inputStream = new FileInputStream( path );
            }

            if ( inputStream == null ) {
                log.error( "Unable to get resource stream for file {}", path );
                return Stream.empty();
            }

            InputStreamReader inputStreamReader = new InputStreamReader( inputStream, StandardCharsets.UTF_8 );
            BufferedReader bufferedReader = new BufferedReader( inputStreamReader );
            return bufferedReader.lines();
        }
        catch ( FileNotFoundException E ) {
            log.error("Unable find file {}", path);
        }

        return Stream.empty();
    }
}
