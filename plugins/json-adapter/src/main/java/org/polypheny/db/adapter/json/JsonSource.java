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
import java.util.List;
import java.util.Map;
import lombok.experimental.Delegate;
import org.pf4j.Extension;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
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
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Sources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
@AdapterProperties(
        name = "JSON",
        description = "An adapter for querying JSON files. A JSON file can be specified by path. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingDirectory(name = "jsonFile", defaultValue = "classpath://articles.json", description = "Path to the JSON file which is to be integrated as this source.", position = 1)
public class JsonSource extends DataSource<DocAdapterCatalog> {

    private static final Logger log = LoggerFactory.getLogger( JsonSource.class );
    @Delegate(excludes = Excludes.class)
    private final DocumentScanDelegate delegate;
    private JsonNamespace namespace;

    private URL jsonFile;


    public JsonSource( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true, new DocAdapterCatalog( storeId ) );
        //this.jsonFile = getJsonFileUrl( settings );
        URL url = getJsonFileUrl( "classpath://articles.json" );
        this.jsonFile = url;
        this.delegate = new DocumentScanDelegate( this, getAdapterCatalog() );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            //this.jsonFile = getJsonFileUrl( settings.get( "jsonFile" ) );
            this.jsonFile = getJsonFileUrl( "classpath://articles.json" );
        }
    }


    private URL getJsonFileUrl( String file ) {
        if ( file.startsWith( "classpath://" ) ) {
            return this.getClass().getClassLoader().getResource( file.replace( "classpath://", "" ) + "/" );
        }
        try {
            return new File( file ).toURI().toURL();
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public void updateNamespace( String name, long id ) {
        // TODO: Ask David. What is name used for?
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
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        if ( !Sources.of( jsonFile ).file().isFile() ) {
            throw new RuntimeException( "File must be a single JSON file, not a directory." );
        }
        try {
            String namespaceName = "foo"; //TODO: Where do i get this from or where is it set?
            return JsonMetaRetriever.getFields( jsonFile, namespaceName );
        } catch ( IOException e ) {
            throw new RuntimeException( "Failed to retrieve columns from json file." );
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
        PhysicalCollection physicalCollection = new JsonCollection.Builder()
                .url( jsonFile )
                .collectionId( collection.getId() )
                .allocationId( allocation.getId() )
                .logicalId( collection.getLogicalId() )
                .namespaceId( namespace.getId() )
                .collectionName( collection.getName() )
                .namespaceName( namespace.getName() )
                .adapter( this )
                .build();
        adapterCatalog.addPhysical( allocation, physicalCollection );
    }

    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method createTable()." );
        return null;
    }

    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalCollection collection = adapterCatalog.createCollection(
                logical.getNamespaceName(),
                logical.getName(),
                logical,
                allocation
        );
        PhysicalCollection physicalCollection = new JsonCollection.Builder()
                .url( jsonFile )
                .collectionId( collection.getId() )
                .allocationId( allocation.getId() )
                .logicalId( collection.getLogicalId() )
                .namespaceId( namespace.getId() )
                .collectionName( collection.getName() )
                .namespaceName( namespace.getName() )
                .adapter( this )
                .build();
        adapterCatalog.replacePhysical( physicalCollection );
        return List.of( physicalCollection );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        // TODO: What is this supposed to do?
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        // TODO: Ask David: Why is this part of this interface?
        log.debug( "NOT SUPPORTED: JSON source does not support method logicalColumn()" );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        log.debug( "NOT SUPPORTED: JSON source does not support method commit()." );
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


    private interface Excludes {

        void refreshCollection( long allocId );

        void createCollection( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

        void restoreCollection( AllocationTable alloc, List<PhysicalEntity> entities );
    }
}


