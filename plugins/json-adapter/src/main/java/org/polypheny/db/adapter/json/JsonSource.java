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

package org.polypheny.db.adapter.json;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.experimental.Delegate;
import org.pf4j.Extension;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentDataSource;
import org.polypheny.db.adapter.DocumentScanDelegate;
import org.polypheny.db.adapter.Scannable;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.catalog.Catalog;
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
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.transaction.PolyXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
@AdapterProperties(
        name = "JSON",
        description = "An adapter for querying JSON files. A single JSON file or a directory containing multiple JSON files can be specified by path. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "link", "url" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
//@AdapterSettingDirectory(subOf = "method_upload", name = "directory", defaultValue = "classpath://articles.json", description = "Path to the JSON file(s) to be integrated as this source.", position = 2)
@AdapterSettingString(subOf = "method_link", defaultValue = "classpath://articles.json", name = "directoryName", description = "Path to the JSON file(s) to be integrated as this source.", position = 2)
@AdapterSettingString(subOf = "method_url", defaultValue = "http://localhost/articles.json", name = "url", description = "URL to the JSON file(s) to be integrated as this source.", position = 2)

public class JsonSource extends DataSource<DocAdapterCatalog> implements DocumentDataSource, Scannable {

    private static final Logger log = LoggerFactory.getLogger( JsonSource.class );
    @Delegate(excludes = Excludes.class)
    private final DocumentScanDelegate delegate;
    private JsonNamespace namespace;
    private final ConnectionMethod connectionMethod;
    private URL jsonFiles;


    public JsonSource( final long storeId, final String uniqueName, final Map<String, String> settings, DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, true, new DocAdapterCatalog( storeId ), Set.of( DataModel.DOCUMENT ) );
        this.connectionMethod = settings.containsKey( "method" ) ? ConnectionMethod.from( settings.get( "method" ).toUpperCase() ) : ConnectionMethod.UPLOAD;
        this.jsonFiles = getJsonFilesUrl( settings );
        this.delegate = new DocumentScanDelegate( this, getAdapterCatalog() );
        long namespaceId = Catalog.getInstance().createNamespace( uniqueName, DataModel.DOCUMENT, true );
        this.namespace = new JsonNamespace( uniqueName, namespaceId, getAdapterId() );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            this.jsonFiles = getJsonFilesUrl( settings );
        }
    }


    private URL getJsonFilesUrl( final Map<String, String> settings ) {
        return switch ( connectionMethod ) {
            case LINK -> {
                String files = settings.get( "directoryName" );
                if ( files.startsWith( "classpath://" ) ) {
                    yield this.getClass().getClassLoader().getResource( files.replace( "classpath://", "" ) );
                }
                try {
                    yield new File( files ).toURI().toURL();
                } catch ( MalformedURLException e ) {
                    throw new GenericRuntimeException( e );
                }
            }
            case UPLOAD -> {
                String files = settings.get( "directory" );
                if ( files.startsWith( "classpath://" ) ) {
                    yield this.getClass().getClassLoader().getResource( files.replace( "classpath://", "" ) + "/" );
                }
                try {
                    yield new File( files ).toURI().toURL();
                } catch ( MalformedURLException e ) {
                    throw new GenericRuntimeException( e );
                }
            }
            case URL -> {
                String files = settings.get( "url" );
                try {
                    yield new URL( files );
                } catch ( MalformedURLException e ) {
                    throw new GenericRuntimeException( e );
                }
            }
        };
    }


    @Override
    public void updateNamespace( String name, long id ) {
        namespace = new JsonNamespace( name, id, adapterId );
    }


    @Override
    public Namespace getCurrentNamespace() {
        return namespace;
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    public List<ExportedDocument> getExportedCollection() {
        try {
            return JsonMetaRetriever.getDocuments( jsonFiles );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Failed to retrieve documents from json file." );
        }
    }


    @Override
    public AdapterCatalog getCatalog() {
        return adapterCatalog;
    }


    @Override
    public void restoreCollection( AllocationCollection allocation, List<PhysicalEntity> entities, Context context ) {
        PhysicalEntity collection = entities.get( 0 ); // TODO: set breakpoint and take a look at what's in this list...
        updateNamespace( collection.getNamespaceName(), collection.getNamespaceId() );
        try {
            PhysicalCollection physicalCollection = new JsonCollection( JsonMetaRetriever.findDocumentUrl( jsonFiles, collection.getName() ), collection, allocation.getId(), namespace, this );
            adapterCatalog.addPhysical( allocation, physicalCollection );
        } catch ( MalformedURLException | NoSuchFileException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalCollection collection = adapterCatalog.createCollection(
                logical.getNamespaceName(),
                logical.getName(),
                logical,
                allocation
        );
        try {
            PhysicalCollection physicalCollection = new JsonCollection( JsonMetaRetriever.findDocumentUrl( jsonFiles, collection.getName() ), collection, allocation.getId(), namespace, this );
            adapterCatalog.replacePhysical( physicalCollection );
            return List.of( physicalCollection );
        } catch ( MalformedURLException | NoSuchFileException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        // TODO: What is this supposed to do?
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        // TODO: Ask David: Why is this part of this interface?
        log.debug( "NOT SUPPORTED: JSON source does not support method renameLogicalColumn()" );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method truncate()." );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method rollback()." );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method dropTable()" );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method createTable()." );
        return null;
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method restoreTable()." );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method createGraph()" );
        return null;
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method dropGraph()" );
    }


    @Override
    public void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method restoreGraph()." );
    }


    @Override
    public DocumentDataSource asDocumentDataSource() {
        return this;
    }


    private interface Excludes {

        void refreshCollection( long allocId );

        void createCollection( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

        void restoreCollection( AllocationTable alloc, List<PhysicalEntity> entities );

    }

}


