/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.xml;

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
        name = "XML",
        description = "An adapter for querying XML files. An XML file or a directory containing multiple XML files can be specified by path. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "link", "url" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
//@AdapterSettingDirectory(subOf = "method_upload", name = "directory", defaultValue = "classpath://products.xml", description = "Path to the XML file(s) to be integrated as this source.", position = 2)
@AdapterSettingString(subOf = "method_link", defaultValue = "classpath://products.xml", name = "directoryName", description = "Path to the XML file(s) to be integrated as this source.", position = 2)
@AdapterSettingString(subOf = "method_url", defaultValue = "http://localhost/cars.xml", name = "url", description = "URL to the XML file(s) to be integrated as this source.", position = 2)

public class XmlSource extends DataSource<DocAdapterCatalog> implements DocumentDataSource, Scannable {

    private static final Logger log = LoggerFactory.getLogger( XmlSource.class );
    @Delegate(excludes = Excludes.class)
    private final DocumentScanDelegate delegate;
    private XmlNamespace namespace;
    private final ConnectionMethod connectionMethod;
    private URL xmlFiles;


    public XmlSource( final long storeId, final String uniqueName, final Map<String, String> settings, DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, true, new DocAdapterCatalog( storeId ), Set.of( DataModel.DOCUMENT ) );
        this.connectionMethod = settings.containsKey( "method" ) ? ConnectionMethod.from( settings.get( "method" ).toUpperCase() ) : ConnectionMethod.UPLOAD;
        this.xmlFiles = getXmlFilesUrl( settings );
        this.delegate = new DocumentScanDelegate( this, getAdapterCatalog() );
        long namespaceId = Catalog.getInstance().createNamespace( uniqueName, DataModel.DOCUMENT, true, false );
        this.namespace = new XmlNamespace( uniqueName, namespaceId, getAdapterId() );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            this.xmlFiles = getXmlFilesUrl( settings );
        }
    }


    private URL getXmlFilesUrl( final Map<String, String> settings ) {
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
        namespace = new XmlNamespace( name, id, adapterId );
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
    public List<ExportedDocument> getExportedCollections() {
        try {
            return XmlMetaRetriever.getDocuments( xmlFiles );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Failed to retrieve documents from XML file." );
        }
    }


    @Override
    public AdapterCatalog getCatalog() {
        return adapterCatalog;
    }


    @Override
    public void restoreCollection( AllocationCollection allocation, List<PhysicalEntity> entities, Context context ) {
        PhysicalEntity collection = entities.get( 0 );
        updateNamespace( collection.getNamespaceName(), collection.getNamespaceId() );
        try {
            PhysicalCollection physicalCollection = new XmlCollection( XmlMetaRetriever.findDocumentUrl( xmlFiles, collection.getName() ), collection, allocation.getId(), namespace, this );
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
            PhysicalCollection physicalCollection = new XmlCollection( XmlMetaRetriever.findDocumentUrl( xmlFiles, collection.getName() ), collection, allocation.getId(), namespace, this );
            adapterCatalog.replacePhysical( physicalCollection );
            return List.of( physicalCollection );
        } catch ( MalformedURLException | NoSuchFileException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        log.debug( "NOT SUPPORTED: XML source does not support method renameLogicalColumn()" );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        log.debug( "NOT SUPPORTED: XML source does not support method truncate()." );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "NOT SUPPORTED: XML source does not support method prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "NOT SUPPORTED: XML source does not support method commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "NOT SUPPORTED: XML source does not support method rollback()." );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        log.debug( "NOT SUPPORTED: XML source does not support method dropTable()" );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        log.debug( "NOT SUPPORTED: XML source does not support method createTable()." );
        return null;
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: XML source does not support method restoreTable()." );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: XML source does not support method createGraph()" );
        return null;
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: XML source does not support method dropGraph()" );
    }


    @Override
    public void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: XML source does not support method restoreGraph()." );
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
