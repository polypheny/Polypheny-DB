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

package org.polypheny.db.adapter.parquet.document;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.experimental.Delegate;
import org.pf4j.Extension;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DocumentDataSource;
import org.polypheny.db.adapter.DocumentScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;

@Extension
@AdapterProperties(
        name = ParquetDocumentSource.NAME,
        description = "A document adapter for querying Parquet files as read-only document collections.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "upload", "link", "url" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
@AdapterSettingDirectory(subOf = "method_upload", name = "directory", defaultValue = "classpath://orders_db", description = "You can upload one or multiple .parquet files.", position = 2)
@AdapterSettingString(subOf = "method_link", defaultValue = "classpath://orders_db", name = "directoryName", description = "You can select a path to a folder or specific .parquet files.", position = 2)
@AdapterSettingString(subOf = "method_url", defaultValue = "", name = "url", description = "URL to the Parquet file(s) to be integrated as this source.", position = 2)
public class ParquetDocumentSource extends AbstractParquetSource implements DocumentDataSource {

    @Delegate(excludes = Excludes.class)
    private final DocumentScanDelegate delegate;

    public static final String NAME = "Parquet Document";


    public ParquetDocumentSource( long storeId, String uniqueName, Map<String, String> settings, DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, Set.of( DataModel.DOCUMENT ) );
        this.delegate = new DocumentScanDelegate( this, adapterCatalog );
    }


    @Override
    public DocumentDataSource asDocumentDataSource() {
        return this;
    }


    @Override
    public List<ExportedDocument> getExportedCollections() {
        return getExportedColumns().keySet().stream()
                .map( name -> new ExportedDocument( name, false, EntityType.SOURCE ) )
                .toList();
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalCollection collection = adapterCatalog.createCollection(
                logical.getNamespaceName(),
                logical.getName(),
                logical,
                allocation );

        var physical = currentNamespace.createParquetCollection( collection, this );
        adapterCatalog.replacePhysical( physical );
        return List.of( physical );
    }


    @Override
    public void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities, Context context ) {
        PhysicalEntity collection = entities.get( 0 );
        updateNamespace( collection.namespaceName, collection.namespaceId );
        var physical = currentNamespace.createParquetCollection( collection.unwrapOrThrow( PhysicalCollection.class ), this );
        adapterCatalog.addPhysical( alloc, physical );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        adapterCatalog.removeAllocAndPhysical( allocation.id );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        log.debug( "NOT SUPPORTED: document Parquet source does not support method createTable()." );
        return null;
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: document Parquet source does not support method restoreTable()." );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        log.debug( "NOT SUPPORTED: document Parquet source does not support method dropTable()." );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: Parquet document source does not support method createGraph()." );
        return null;
    }


    @Override
    public void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        log.debug( "NOT SUPPORTED: Parquet document source does not support method restoreGraph()." );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        log.debug( "NOT SUPPORTED: Parquet document source does not support method dropGraph()." );
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        adapterCatalog.renameLogicalColumn( id, newColumnName );
    }


    @SuppressWarnings("unused")
    private interface Excludes {

        void renameLogicalColumn( long id, String newColumnName );

        void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation );

        void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities, Context context );

        void dropCollection( Context context, AllocationCollection allocation );

    }

}
