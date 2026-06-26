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

package org.polypheny.db.adapter.mongodb.source;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Set;
import lombok.experimental.Delegate;
import org.bson.Document;
import org.pf4j.Extension;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentDataSource;
import org.polypheny.db.adapter.DocumentScanDelegate;
import org.polypheny.db.adapter.Scannable;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.adapter.mongodb.MongoNamespace;
import org.polypheny.db.adapter.mongodb.TransactionProvider;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.transaction.PolyXid;

@Extension
@AdapterProperties(
        name = "MongoDB",
        description = "MongoDB is a document-oriented database system.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "host", defaultValue = "localhost", description = "Hostname or IP address of the remote MongoDB instance.")
@AdapterSettingInteger(name = "port", defaultValue = 27017, description = "Port number on the remote MongoDB instance.")
@AdapterSettingString(name = "database", defaultValue = "public", description = "Name of the database to connect to.")
@AdapterSettingString(name = "username", defaultValue = "", description = "Optional username for authenticating at the remote MongoDB instance.")
@AdapterSettingString(name = "password", defaultValue = "", description = "Optional password for authenticating at the remote MongoDB instance.")
@AdapterSettingString(name = "authSource", defaultValue = "", description = "Optional authentication database. If empty, the selected database is used.")
public class MongoSource extends DataSource<DocAdapterCatalog> implements DocumentDataSource, Scannable {

    @Delegate(excludes = Excludes.class)
    private final DocumentScanDelegate delegate;
    private final String database;
    private final transient MongoClient client;
    private final transient TransactionProvider transactionProvider;
    private transient MongoNamespace currentNamespace;


    public MongoSource( final long adapterId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super( adapterId, uniqueName, settings, mode, true, new DocAdapterCatalog( adapterId ), Set.of( DataModel.DOCUMENT ) );
        if ( mode != DeployMode.REMOTE ) {
            throw new GenericRuntimeException( "Not supported deploy mode: " + mode.name() );
        }

        String host = settings.get( "host" );
        int port = Integer.parseInt( settings.get( "port" ) );
        this.database = settings.get( "database" );
        String username = settings.getOrDefault( "username", "" );
        String password = settings.getOrDefault( "password", "" );
        String authSource = settings.getOrDefault( "authSource", "" );

        MongoClientSettings.Builder mongoSettingsBuilder = MongoClientSettings
                .builder()
                .applyToClusterSettings( builder ->
                        builder.hosts( List.of( new ServerAddress( host, port ) ) )
                );
        if ( !username.isBlank() ) {
            String effectiveAuthSource = authSource.isBlank() ? this.database : authSource;
            mongoSettingsBuilder.credential( MongoCredential.createCredential( username, effectiveAuthSource, password.toCharArray() ) );
        }
        MongoClientSettings mongoSettings = mongoSettingsBuilder.build();
        this.client = MongoClients.create( mongoSettings );

        addInformationPhysicalNames();
        enableInformationPage();

        this.transactionProvider = new TransactionProvider( this.client );

        testConnection();

        this.delegate = new DocumentScanDelegate( this, adapterCatalog );

    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // The source has no live connection yet.
    }


    @Override
    public void updateNamespace( String name, long id ) {
        currentNamespace = new MongoNamespace( id, database.toLowerCase( Locale.ROOT ), client, transactionProvider, this );
        putNamespace( currentNamespace );
    }


    @Override
    public Namespace getCurrentNamespace() {
        return currentNamespace;
    }


    @Override
    public void shutdown() {
        removeInformationPage();
        client.close();
    }


    @Override
    public List<ExportedDocument> getExportedCollections() {
        MongoDatabase mongoDatabase = client.getDatabase( database );
        List<String> collectionNames = mongoDatabase.listCollectionNames()
                .into( new java.util.ArrayList<>() )
                .stream().toList();
        return collectionNames.stream()
                .map( name -> new ExportedDocument( name, false, EntityType.SOURCE ) )
                .toList();
    }


    @Override
    public boolean supportsDynamicCollectionDiscovery() {
        return true;
    }


    @Override
    public AdapterCatalog getCatalog() {
        return adapterCatalog;
    }


    @Override
    public void truncate( Context context, long allocId ) {
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        // no-op
    }


    @Override
    public void rollback( PolyXid xid ) {
        // no-op
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        throw new GenericRuntimeException( "Mongo source does not support dropTable()." );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        throw new GenericRuntimeException( "Mongo source does not support createTable()." );
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        throw new GenericRuntimeException( "Mongo source does not support restoreTable()." );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        throw new GenericRuntimeException( "Mongo source does not support createGraph()." );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        throw new GenericRuntimeException( "Mongo source does not support dropGraph()." );
    }


    @Override
    public void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        throw new GenericRuntimeException( "Mongo source does not support restoreGraph()." );
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        if ( currentNamespace == null || currentNamespace.getId() != logical.namespaceId ) {
            updateNamespace( logical.getNamespaceName(), logical.namespaceId );
        }

        PhysicalCollection collection = adapterCatalog.createCollection(
                logical.getNamespaceName(),
                logical.getName(),
                logical,
                allocation );

        MongoEntity physicalCollection = currentNamespace.createEntity( collection, List.of() );
        adapterCatalog.replacePhysical( physicalCollection );
        return List.of( physicalCollection );
    }


    @Override
    public void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities, Context context ) {
        PhysicalEntity collection = entities.get( 0 );
        updateNamespace( collection.getNamespaceName(), collection.getNamespaceId() );
        adapterCatalog.addPhysical( alloc, currentNamespace.createEntity( collection, List.of() ) );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        adapterCatalog.removeAllocAndPhysical( allocation.id );
    }


    @Override
    public DocumentDataSource asDocumentDataSource() {
        return this;
    }


    private interface Excludes {

        void refreshCollection( long allocId );

        void createCollection( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

        void dropCollection( Context context, AllocationCollection allocation );

        void restoreCollection( AllocationTable alloc, List<PhysicalEntity> entities );

    }


    private void testConnection() {
        try {
            client.getDatabase( database ).runCommand( new Document( "ping", 1 ) );
            List<String> databaseNames = client.listDatabaseNames().into( new ArrayList<>() );
            List<String> collectionNames = getExportedCollections().stream().map( ExportedDocument::name ).toList();
            if ( !databaseNames.contains( database ) && collectionNames.isEmpty() ) {
                throw new GenericRuntimeException( "MongoDB database does not exist or is empty: " + database );
            }
        } catch ( GenericRuntimeException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to connect to MongoDB source", e );
        }
    }

}
