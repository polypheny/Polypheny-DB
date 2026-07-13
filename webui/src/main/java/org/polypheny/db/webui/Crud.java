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

package org.polypheny.db.webui;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.Part;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSettingDirectory;
import org.polypheny.db.adapter.AbstractAdapterSettingString;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.FunctionalIndexInfo;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.RelationalDataSource.ExportedForeignKey;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.NameGenerator;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.SourceRefreshDetails;
import org.polypheny.db.ddl.DdlManager.SourceRefreshSummary;
import org.polypheny.db.docker.AutoDocker;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerSetupHelper;
import org.polypheny.db.docker.HandshakeManager;
import org.polypheny.db.docker.exceptions.DockerUserException;
import org.polypheny.db.docker.models.AutoDockerResult;
import org.polypheny.db.docker.models.CreateDockerRequest;
import org.polypheny.db.docker.models.CreateDockerResponse;
import org.polypheny.db.docker.models.DockerSettings;
import org.polypheny.db.docker.models.HandshakeInfo;
import org.polypheny.db.docker.models.InstancesAndAutoDocker;
import org.polypheny.db.docker.models.UpdateDockerRequest;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceCreateRequest;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.partition.PartitionFunctionInfo;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.plugins.PolyPluginManager.PluginStatus;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.security.SecurityManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.FileInputHandle;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.webui.auth.AuthCrud;
import org.polypheny.db.webui.crud.CatalogCrud;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.crud.LanguageCrud.TriFunction;
import org.polypheny.db.webui.crud.StatisticCrud;
import org.polypheny.db.webui.models.DbTable;
import org.polypheny.db.webui.models.ForeignKey;
import org.polypheny.db.webui.models.IndexAdapterModel;
import org.polypheny.db.webui.models.IndexAdapterModel.IndexMethodModel;
import org.polypheny.db.webui.models.IndexModel;
import org.polypheny.db.webui.models.MaterializedInfos;
import org.polypheny.db.webui.models.Namespace;
import org.polypheny.db.webui.models.PartitionFunctionModel;
import org.polypheny.db.webui.models.PartitionFunctionModel.FieldType;
import org.polypheny.db.webui.models.PartitionFunctionModel.PartitionFunctionColumn;
import org.polypheny.db.webui.models.PathAccessRequest;
import org.polypheny.db.webui.models.PlacementFieldsModel;
import org.polypheny.db.webui.models.PlacementModel;
import org.polypheny.db.webui.models.PlacementModel.RelationalStore;
import org.polypheny.db.webui.models.QueryInterfaceModel;
import org.polypheny.db.webui.models.SidebarElement;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.TableConstraint;
import org.polypheny.db.webui.models.Uml;
import org.polypheny.db.webui.models.UnderlyingTables;
import org.polypheny.db.webui.models.catalog.AdapterModel;
import org.polypheny.db.webui.models.catalog.PolyTypeModel;
import org.polypheny.db.webui.models.catalog.SnapshotModel;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest.Update;
import org.polypheny.db.webui.models.requests.ColumnRequest;
import org.polypheny.db.webui.models.requests.ConstraintRequest;
import org.polypheny.db.webui.models.requests.EditTableRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest.ModifyPartitionRequest;
import org.polypheny.db.webui.models.requests.PolyAlgRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.RenameEntityRequest;
import org.polypheny.db.webui.models.requests.SourceMaterializationRequest;
import org.polypheny.db.webui.models.requests.SourceRefreshRequest;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.requests.UpdateAdapterRequest;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.RelationalResult.RelationalResultBuilder;
import org.polypheny.db.webui.models.results.QueryType;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.webui.models.results.Result.ResultBuilder;
import org.polypheny.db.webui.models.results.ResultType;


@Getter
@Slf4j
public class Crud implements InformationObserver, PropertyChangeListener {

    private static final Gson gson = new Gson();
    public static final String ORIGIN = "Polypheny-UI";
    private static final int POSTGRES_MAX_VARCHAR_LENGTH = 10_485_760;
    private static final int DOCUMENT_MATERIALIZATION_COPY_BATCH_SIZE = 5_000;
    private static final int RELATIONAL_MATERIALIZATION_COPY_BATCH_SIZE = 10_000;
    private final TransactionManager transactionManager;

    public final LanguageCrud languageCrud;
    public final StatisticCrud statisticCrud;

    public final CatalogCrud catalogCrud;
    public final AuthCrud authCrud;


    /**
     * Constructor
     *
     * @param transactionManager The Polypheny-DB transaction manager
     */
    Crud( final TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.languageCrud = new LanguageCrud( this );
        this.statisticCrud = new StatisticCrud( this );
        this.catalogCrud = new CatalogCrud( this );
        this.authCrud = new AuthCrud( this );

        Catalog.afterInit( () -> Catalog.getInstance().addObserver( this ) );
    }


    /**
     * Closes analyzers and deletes temporary files.
     */
    public static void cleanupOldSession( ConcurrentHashMap<String, Set<String>> sessionXIds, final String sessionId ) {
        Set<String> xIds = sessionXIds.remove( sessionId );
        if ( xIds == null || xIds.isEmpty() ) {
            return;
        }
        for ( String xId : xIds ) {
            InformationManager.close( xId );
            TemporalFileManager.deleteFilesOfTransaction( xId );
        }
    }


    /**
     * Refreshes the schema of a source table if it is out of sync with the underlying data source.
     * <p>
     * The method retrieves the current table metadata from the catalog and compares it with the
     * actual source schema obtained via the adapter. If new columns are detected in the source,
     * they are added to the corresponding Polypheny table. If columns no longer exist in the source,
     * they are removed from the corresponding Polypheny table.
     *
     * @param request UI request containing the target entity identifier
     * @throws GenericRuntimeException if the refresh fails
     */
    public SourceMaterializationRefreshResult refreshSourceSchemaIfNeeded( UIRequest request ) {
        Transaction transaction = getTransaction();
        boolean refreshSynchronizedData = "synchronizedApplyWithData".equalsIgnoreCase( request.refreshTrigger );
        LogicalTable synchronizedMaterialization = null;
        boolean committed = false;
        try {
            Statement ddlStatement = transaction.createStatement();
            LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElse( null );
            List<String> changeDescriptions;
            boolean sourceEntityDeleted = false;
            if ( table != null && table.synchronizedSourceEntityId != null ) {
                synchronizedMaterialization = table;
                List<String> previewChangeDescriptions = DdlManager.getInstance().previewSynchronizedSourceMaterializationRefresh( request.entityId );
                sourceEntityDeleted = hasSourceDeletedChange( previewChangeDescriptions );
                if ( sourceEntityDeleted ) {
                    changeDescriptions = previewChangeDescriptions;
                } else if ( "synchronizedApply".equalsIgnoreCase( request.refreshTrigger ) || (refreshSynchronizedData && request.confirmedDataRefresh) ) {
                    DdlManager.getInstance().refreshSourceSchemaIfNeeded( table.synchronizedSourceEntityId, ddlStatement );
                    changeDescriptions = DdlManager.getInstance().refreshSynchronizedSourceMaterializationColumns( request.entityId, ddlStatement );
                } else {
                    changeDescriptions = previewChangeDescriptions;
                }
            } else {
                changeDescriptions = DdlManager.getInstance().refreshSourceSchemaIfNeeded( request.entityId, ddlStatement );
                sourceEntityDeleted = hasSourceDeletedChange( changeDescriptions );
            }
            transaction.commit();
            committed = true;
            if ( refreshSynchronizedData && synchronizedMaterialization != null && !sourceEntityDeleted ) {
                long rowCount = countSynchronizedSourceMaterializationRows( synchronizedMaterialization.id );
                if ( !request.confirmedDataRefresh ) {
                    return new SourceMaterializationRefreshResult( changeDescriptions, rowCount, sourceEntityDeleted );
                }
                refreshSynchronizedSourceMaterializationData( synchronizedMaterialization.id );
                changeDescriptions = new ArrayList<>( changeDescriptions );
                changeDescriptions.add( "Refreshed data from source" );
            }
            return new SourceMaterializationRefreshResult( changeDescriptions, null, sourceEntityDeleted );
        } catch ( Exception e ) {
            if ( !committed ) {
                try {
                    transaction.rollback( "Error while refreshing source catalog: " + e.getMessage() );
                } catch ( Exception rollbackException ) {
                    log.error( "Rollback also failed", rollbackException );
                }
            }
            throw new GenericRuntimeException(
                    "Could not refresh source catalog for entity " + request.entityId, e );
        }
    }


    public record SourceMaterializationRefreshResult( List<String> changeDescriptions, Long dataRefreshRowCount, boolean sourceEntityDeleted ) {

    }


    private boolean hasSourceDeletedChange( List<String> changeDescriptions ) {
        return changeDescriptions.stream().anyMatch( description -> description.startsWith( "Source " ) && description.endsWith( " was deleted in the source." ) );
    }


    public SourceMaterializationRefreshResult refreshSourceCollectionIfNeeded( UIRequest request ) {
        Transaction transaction = getTransaction();
        boolean committed = false;
        try {
            Statement ddlStatement = transaction.createStatement();
            boolean sourceEntityDeleted = false;
            List<String> changeDescriptions = DdlManager.getInstance().refreshSourceCollectionIfNeeded( request.entityId, ddlStatement );
            sourceEntityDeleted = Catalog.snapshot().doc().getCollection( request.entityId ).isEmpty();
            transaction.commit();
            committed = true;
            return new SourceMaterializationRefreshResult( changeDescriptions, null, sourceEntityDeleted );
        } catch ( Exception e ) {
            if ( !committed ) {
                try {
                    transaction.rollback( "Error while refreshing source collection catalog: " + e.getMessage() );
                } catch ( Exception rollbackException ) {
                    log.error( "Rollback also failed", rollbackException );
                }
            }
            throw new GenericRuntimeException(
                    "Could not refresh source collection catalog for entity " + request.entityId, e );
        }
    }


    private long countSynchronizedSourceMaterializationRows( long materializedTableId ) {
        Snapshot snapshot = Catalog.snapshot();
        LogicalTable materializedTable = snapshot.rel().getTable( materializedTableId ).orElseThrow();
        if ( materializedTable.synchronizedSourceEntityId == null ) {
            return 0;
        }
        LogicalTable sourceTable = snapshot.rel().getTable( materializedTable.synchronizedSourceEntityId ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceTable.namespaceId ).orElseThrow();
        String sourceTableName = quoteQualified( sourceNamespace.name, sourceTable.name );
        RelationalResult countResult = (RelationalResult) executeSql( "SELECT COUNT(*) FROM " + sourceTableName );
        if ( countResult.error != null ) {
            throw new GenericRuntimeException( countResult.error );
        }
        if ( countResult.data == null || countResult.data.length == 0 || countResult.data[0].length == 0 ) {
            return 0;
        }
        return Long.parseLong( countResult.data[0][0] );
    }


    public void refreshSelectedSources( final Context ctx ) {
        SourceRefreshRequest request = ctx.bodyAsClass( SourceRefreshRequest.class );
        List<String> sourceNames = request.getSourceIds().stream()
                .map( sourceId -> Catalog.snapshot().getAdapter( sourceId ).map( a -> a.uniqueName ).orElse( String.valueOf( sourceId ) ) )
                .toList();
        log.info( "Received a source refresh request for source(s) {}", sourceNames );
        SourceRefreshDetails refreshDetails = refreshSelectedSourcesWithDetails( request.getSourceIds() );
        List<Map<String, Object>> refreshSummaries = refreshDetails.summaries().stream()
                .map( summary -> Map.<String, Object>of(
                        "sourceName", summary.sourceName(),
                        "entityName", summary.entityName(),
                        "dataModel", summary.dataModel(),
                        "changeDescriptions", summary.changeDescriptions() ) )
                .toList();
        ctx.json( Map.of(
                "success", true,
                "refreshedSources", refreshDetails.refreshedSources(),
                "refreshedCount", refreshDetails.refreshedSources().size(),
                "refreshSummaries", refreshSummaries ) );
    }


    public void refreshSourcesForQuery( final Context ctx ) {
        QueryRequest request = ctx.bodyAsClass( QueryRequest.class );
        List<SourceRefreshSummary> summaries = refreshSourcesForQuery( request );
        List<Map<String, Object>> refreshSummaries = summaries.stream()
                .map( summary -> Map.<String, Object>of(
                        "sourceName", summary.sourceName(),
                        "entityName", summary.entityName(),
                        "dataModel", summary.dataModel(),
                        "changeDescriptions", summary.changeDescriptions() ) )
                .toList();
        ctx.json( Map.of(
                "success", true,
                "refreshedSources", summaries.stream().map( SourceRefreshSummary::entityName ).toList(),
                "refreshedCount", summaries.size(),
                "refreshSummaries", refreshSummaries ) );
    }


    public List<SourceRefreshSummary> refreshSourcesForQuery( QueryRequest request ) {
        Snapshot snapshot = Catalog.snapshot();
        long namespaceId = LanguageCrud.getNamespaceIdOrDefault( request.namespace );
        List<LogicalTable> referencedTables = SourceQueryReferenceDetector.referencedSourceTables( request.query, request.language, namespaceId, snapshot );
        List<LogicalCollection> referencedCollections = SourceQueryReferenceDetector.referencedSourceCollections( request.query, request.language, namespaceId, snapshot );

        if ( referencedTables.isEmpty() && referencedCollections.isEmpty() ) {
            return List.of();
        }

        Transaction transaction = getTransaction();
        try {
            Statement ddlStatement = transaction.createStatement();
            List<SourceRefreshSummary> summaries = new ArrayList<>();
            for ( LogicalTable table : referencedTables ) {
                List<String> changeDescriptions = DdlManager.getInstance().refreshSourceSchemaIfNeeded( table.id, ddlStatement );
                if ( !changeDescriptions.isEmpty() ) {
                    summaries.add( new SourceRefreshSummary( getSourceNameForEntity( table.id, snapshot ), table.name, DataModel.RELATIONAL, changeDescriptions ) );
                }
            }
            for ( LogicalCollection collection : referencedCollections ) {
                List<String> changeDescriptions = DdlManager.getInstance().refreshSourceCollectionIfNeeded( collection.id, ddlStatement );
                if ( !changeDescriptions.isEmpty() ) {
                    summaries.add( new SourceRefreshSummary( getSourceNameForEntity( collection.id, snapshot ), collection.name, DataModel.DOCUMENT, changeDescriptions ) );
                }
            }
            transaction.commit();
            return summaries;
        } catch ( Exception e ) {
            try {
                transaction.rollback( "Error while refreshing sources for query: " + e.getMessage() );
            } catch ( Exception rollbackException ) {
                log.error( "Rollback also failed", rollbackException );
            }
            throw new GenericRuntimeException( "Could not refresh sources for query", e );
        }
    }


    private String getSourceNameForEntity( long entityId, Snapshot snapshot ) {
        return snapshot.alloc().getFromLogical( entityId ).stream()
                .findFirst()
                .flatMap( allocation -> snapshot.getAdapter( allocation.adapterId ) )
                .map( adapter -> adapter.uniqueName )
                .orElse( String.valueOf( entityId ) );
    }


    public List<String> refreshSelectedSources( List<Long> sourceIds ) {
        return refreshSelectedSourcesWithDetails( sourceIds ).refreshedSources();
    }


    public SourceRefreshDetails refreshSelectedSourcesWithDetails( List<Long> sourceIds ) {
        Transaction transaction = getTransaction();
        try {
            Statement ddlStatement = transaction.createStatement();
            SourceRefreshDetails refreshDetails = DdlManager.getInstance().refreshSelectedSourcesWithDetails( sourceIds, ddlStatement );

            transaction.commit();
            List<String> sourceNames = sourceIds.stream()
                    .map( sourceId -> Catalog.snapshot().getAdapter( sourceId ).map( a -> a.uniqueName ).orElse( String.valueOf( sourceId ) ) )
                    .toList();
            log.info( "Schema refresh finished successfully for selected source(s) {}", sourceNames );
            return refreshDetails;
        } catch ( Exception e ) {
            try {
                transaction.rollback( "Error while refreshing selected sources: " + e.getMessage() );
            } catch ( Exception rollbackException ) {
                log.error( "Rollback also failed", rollbackException );
            }
            throw new GenericRuntimeException( "Could not refresh selected sources", e );
        }
    }


    /**
     * Returns the content of a table with a maximum of PAGESIZE elements.
     */
    RelationalResult getTable( final UIRequest request ) {
        Transaction transaction = getTransaction();
        RelationalResultBuilder<?, ?> resultBuilder;
        QueryLanguage language = QueryLanguage.from( "sql" );

        StringBuilder query = new StringBuilder();
        String where = "";
        if ( request.filter != null ) {
            where = filterTable( request.filter );
        }
        String orderBy = "";
        if ( request.sortState != null ) {
            orderBy = sortTable( request.sortState );
        }

        String fullTableName = getFullEntityName( request.entityId );
        query.append( "SELECT * FROM " )
                .append( fullTableName )
                .append( where )
                .append( orderBy );

        TriFunction<ExecutedContext, UIRequest, Statement, ResultBuilder<?, ?, ?, ?>> builder = LanguageCrud.getToResult( language );

        ImplementationContext implementationContext = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query.toString() )
                        .language( language )
                        .transactions( List.of( transaction ) )
                        .origin( transaction.getOrigin() )
                        .batch( request.noLimit ? -1 : getPageSize() )
                        .transactionManager( transactionManager )
                        .build(), transaction ).get( 0 );
        ExecutedContext ec = implementationContext.execute( implementationContext.getStatement() );

        if ( ec.getException().isPresent() ) {
            // TODO: Create a dedicated error result class
            return RelationalResult.builder().exception( ec.getException().get() ).error( ec.getException().get().toString() ).build();
        }

        resultBuilder = (RelationalResultBuilder<?, ?>) builder.apply( ec, request, implementationContext.getStatement() );

        // determine if it is a view or a table
        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();
        resultBuilder.dataModel( table.dataModel );
        if ( table.modifiable ) {
            resultBuilder.type( ResultType.TABLE );
        } else {
            resultBuilder.type( ResultType.VIEW );
        }

        //get headers with default values
        List<UiColumnDefinition> cols = new ArrayList<>();
        List<String> primaryColumns;
        if ( table.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
            primaryColumns = new ArrayList<>( primaryKey.getFieldNames() );
        } else {
            primaryColumns = new ArrayList<>();
        }
        for ( LogicalColumn logicalColumn : Catalog.snapshot().rel().getColumns( table.id ) ) {
            PolyValue defaultValue = logicalColumn.defaultValue == null ? null : logicalColumn.defaultValue.value;
            String collectionsType = logicalColumn.collectionsType == null ? "" : logicalColumn.collectionsType.getName();
            cols.add(
                    UiColumnDefinition.builder()
                            .name( logicalColumn.name )
                            .dataType( logicalColumn.type.getName() )
                            .collectionsType( collectionsType )
                            .nullable( logicalColumn.nullable )
                            .precision( logicalColumn.length )
                            .scale( logicalColumn.scale )
                            .dimension( logicalColumn.dimension )
                            .cardinality( logicalColumn.cardinality )
                            .primary( primaryColumns.contains( logicalColumn.name ) )
                            .defaultValue( defaultValue == null ? null : defaultValue.toJson() )
                            .sort( request.sortState == null ? new SortState() : request.sortState.get( logicalColumn.name ) )
                            .filter( request.filter == null || request.filter.get( logicalColumn.name ) == null ? "" : request.filter.get( logicalColumn.name ) ).build() );
        }
        resultBuilder.header( cols.toArray( new UiColumnDefinition[0] ) );

        resultBuilder.currentPage( request.currentPage ).table( table.name );
        long tableSize = 0;
        try {
            tableSize = getTableSize( transaction, request );
        } catch ( Exception e ) {
            log.error( "Caught exception while determining page size", e );
        }
        resultBuilder.highestPage( (int) Math.ceil( (double) tableSize / getPageSize() ) );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            transaction.rollback( "Caught exception while committing transaction. " + e );
        }
        return resultBuilder.build();
    }


    /**
     * Get all tables of a namespace
     */
    void getEntities( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        long namespaceId = request.namespaceId != null ? request.namespaceId : Catalog.defaultNamespaceId;
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( namespaceId ).orElseThrow();

        List<? extends LogicalEntity> entities = switch ( namespace.dataModel ) {
            case RELATIONAL -> Catalog.snapshot().rel().getTables( namespace.id, null );
            case DOCUMENT -> Catalog.snapshot().doc().getCollections( namespace.id, null );
            case GRAPH -> Catalog.snapshot().graph().getGraphs( null );
        };

        List<DbTable> result = new ArrayList<>();
        for ( LogicalEntity e : entities ) {
            result.add( new DbTable( e.name, namespace.name, e.modifiable, e.entityType ) );
        }
        ctx.json( result );
    }


    void renameTable( final Context ctx ) {
        RenameEntityRequest table = ctx.bodyAsClass( RenameEntityRequest.class );
        if ( !ensureTableModifiable( table.getEntityId(), ctx ) ) {
            return;
        }
        String query = String.format( "ALTER TABLE %s RENAME TO \"%s\"", getFullEntityName( table.getEntityId() ), table.getEntityName() );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> result = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );

        ctx.json( result );
    }


    void renameCollection( final Context ctx ) {
        RenameEntityRequest request = ctx.bodyAsClass( RenameEntityRequest.class );
        LogicalCollection collection = Catalog.snapshot().doc().getCollection( request.getEntityId() ).orElseThrow();
        if ( !ensureCollectionModifiable( collection.id, ctx ) ) {
            return;
        }
        String query = String.format( "db.\"%s\".renameCollection(\"%s\")", collection.name, request.getEntityName() );
        QueryLanguage language = QueryLanguage.from( "mql" );
        Result<?, ?> result = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .namespaceId( request.namespaceId )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );

        ctx.json( result );
    }


    /**
     * Drop or truncate a table
     */
    void dropTruncateTable( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );

        StringBuilder query = new StringBuilder();
        if ( request.tableType != null && request.action.equalsIgnoreCase( "drop" ) && request.tableType == EntityType.VIEW ) {
            query.append( "DROP VIEW " );
        } else if ( request.action.equalsIgnoreCase( "drop" ) ) {
            query.append( "DROP TABLE " );
        } else if ( request.action.equalsIgnoreCase( "truncate" ) ) {
            query.append( "TRUNCATE TABLE " );
        }

        Pair<LogicalNamespace, LogicalTable> namespaceTable = getNamespaceTable( request );

        String fullTableName = String.format( "\"%s\".\"%s\"", namespaceTable.left.name, namespaceTable.right.name );
        query.append( fullTableName );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> result = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query.toString() )
                        .language( language )
                        .userId( Catalog.defaultUserId )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( result );
    }


    private Pair<LogicalNamespace, LogicalTable> getNamespaceTable( EditTableRequest request ) {
        long namespaceId = request.namespaceId == null ? Catalog.defaultNamespaceId : request.namespaceId;
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( namespaceId ).orElseThrow();
        long entityId = request.entityId == null ? -1 : request.entityId;
        LogicalTable table = Catalog.snapshot().rel().getTable( entityId ).orElseThrow();

        return Pair.of( namespace, table );
    }


    private LogicalNamespace getNamespace( EditTableRequest request ) {
        long namespaceId = request.namespaceId == null ? Catalog.defaultNamespaceId : request.namespaceId;

        return Catalog.snapshot().getNamespace( namespaceId ).orElseThrow();
    }


    private String getFullEntityName( long entityId ) {
        LogicalTable table = Catalog.snapshot().rel().getTable( entityId ).orElseThrow();
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( table.namespaceId ).orElseThrow();
        return String.format( "\"%s\".\"%s\"", namespace.name, table.name );
    }


    private boolean ensureTableModifiable( long entityId, Context ctx ) {
        LogicalTable table = Catalog.snapshot().rel().getTable( entityId ).orElseThrow();
        if ( table.modifiable ) {
            return true;
        }
        ctx.json( RelationalResult.builder().error( "Unable to modify a table marked as read-only!" ).build() );
        return false;
    }


    private boolean ensureTableModifiable( String namespaceName, String tableName, Context ctx ) {
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( namespaceName ).orElseThrow();
        LogicalTable table = Catalog.snapshot().rel().getTable( namespace.id, tableName ).orElseThrow();
        return ensureTableModifiable( table.id, ctx );
    }


    private boolean ensureCollectionModifiable( long entityId, Context ctx ) {
        LogicalCollection collection = Catalog.snapshot().doc().getCollection( entityId ).orElseThrow();
        if ( collection.modifiable ) {
            return true;
        }
        ctx.json( RelationalResult.builder().error( "Unable to modify a collection marked as read-only!" ).build() );
        return false;
    }


    /**
     * Create a new table
     */
    void createTable( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );

        StringBuilder query = new StringBuilder();
        StringJoiner colJoiner = new StringJoiner( "," );
        LogicalNamespace namespace = getNamespace( request );

        String fullTableName = String.format( "\"%s\".\"%s\"", namespace.name, request.entityName );
        query.append( "CREATE TABLE " ).append( fullTableName ).append( "(" );
        StringBuilder colBuilder;

        StringJoiner primaryKeys = new StringJoiner( ",", "PRIMARY KEY (", ")" );
        int primaryCounter = 0;
        for ( UiColumnDefinition col : request.columns ) {
            colBuilder = new StringBuilder();
            colBuilder.append( "\"" ).append( col.name ).append( "\" " ).append( col.dataType );
            if ( col.precision != null ) {
                colBuilder.append( "(" ).append( col.precision );
                if ( col.scale != null ) {
                    colBuilder.append( "," ).append( col.scale );
                }
                colBuilder.append( ")" );
            }
            if ( col.collectionsType != null && !col.collectionsType.isEmpty() ) {
                colBuilder.append( " " ).append( col.collectionsType );
                if ( col.dimension != null ) {
                    colBuilder.append( "(" ).append( col.dimension );
                    if ( col.cardinality != null ) {
                        colBuilder.append( "," ).append( col.cardinality );
                    }
                    colBuilder.append( ")" );
                }
            }
            if ( !col.nullable ) {
                colBuilder.append( " NOT NULL" );
            }
            if ( col.defaultValue != null ) {
                switch ( col.dataType ) {
                    //TODO FIX DATA TYPES
                    case "int8":
                    case "int4":
                        int a = Integer.parseInt( col.defaultValue );
                        colBuilder.append( " DEFAULT " ).append( a );
                        break;
                    case "varchar":
                        colBuilder.append( String.format( " DEFAULT '%s'", col.defaultValue ) );
                        break;
                    default:
                        // varchar, timestamp, boolean
                        colBuilder.append( " DEFAULT " ).append( col.defaultValue );
                }
            }
            if ( col.primary ) {
                primaryKeys.add( "\"" + col.name + "\"" );
                primaryCounter++;
            }
            colJoiner.add( colBuilder.toString() );
        }
        if ( primaryCounter > 0 ) {
            colJoiner.add( primaryKeys.toString() );
        }
        query.append( colJoiner );
        query.append( ")" );
        if ( request.storeId != null ) {
            LogicalAdapter adapter = Catalog.snapshot().getAdapter( request.storeId ).orElseThrow();
            query.append( String.format( " ON STORE \"%s\"", adapter.uniqueName ) );
        }
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> result = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query.toString() )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( result );
    }


    void createIndependentSourceMaterialization( final Context ctx ) {
        SourceMaterializationRequest request = ctx.bodyAsClass( SourceMaterializationRequest.class );
        Snapshot snapshot = Catalog.snapshot();
        LogicalTable sourceTable = snapshot.rel().getTable( request.getSourceEntityId() ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceTable.namespaceId ).orElseThrow();
        long targetNamespaceId = request.getTargetNamespaceId() == null ? sourceNamespace.id : request.getTargetNamespaceId();
        LogicalNamespace targetNamespace = snapshot.getNamespace( targetNamespaceId ).orElseThrow();
        LogicalAdapter targetStore = snapshot.getAdapter( request.getTargetStoreId() ).orElseThrow();

        String independentTableName;
        try {
            independentTableName = resolveMaterializationTableName( request.getTargetEntityName(), targetNamespace.id, getNextIndependentMaterializationTableName( targetNamespace.id, sourceTable.name ), "Independent Materialization" );
        } catch ( GenericRuntimeException e ) {
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
            return;
        }
        String targetTable = quoteQualified( targetNamespace.name, independentTableName );
        String sourceTableName = quoteQualified( sourceNamespace.name, sourceTable.name );
        List<LogicalColumn> columns = snapshot.rel().getColumns( sourceTable.id ).stream().sorted().toList();

        String createQuery = buildCreateMaterializationTableQuery( targetTable, targetStore.uniqueName, sourceTable, columns );
        Result<?, ?> createResult = executeSql( createQuery );
        if ( createResult.error != null ) {
            ctx.json( createResult );
            return;
        }

        String copyQueryDescription = buildBatchedRelationalCopyQueryDescription( targetTable, sourceTableName );
        Result<?, ?> insertResult = copyRelationalTableRows( sourceTable, sourceTableName, targetTable, columns );
        if ( insertResult.error != null ) {
            executeSql( "DROP TABLE " + targetTable );
            ctx.json( insertResult );
            return;
        }

        ctx.json( RelationalResult.builder()
                .table( independentTableName )
                .namespace( targetNamespace.name )
                .query( copyQueryDescription )
                .queryType( QueryType.DML )
                .affectedTuples( insertResult.affectedTuples )
                .build() );
    }


    void createSynchronizedSourceMaterialization( final Context ctx ) {
        SourceMaterializationRequest request = ctx.bodyAsClass( SourceMaterializationRequest.class );
        Snapshot snapshot = Catalog.snapshot();
        LogicalTable sourceTable = snapshot.rel().getTable( request.getSourceEntityId() ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceTable.namespaceId ).orElseThrow();
        long targetNamespaceId = request.getTargetNamespaceId() == null ? sourceNamespace.id : request.getTargetNamespaceId();
        LogicalNamespace targetNamespace = snapshot.getNamespace( targetNamespaceId ).orElseThrow();
        LogicalAdapter targetStore = snapshot.getAdapter( request.getTargetStoreId() ).orElseThrow();

        if ( sourceTable.entityType != EntityType.SOURCE ) {
            ctx.json( RelationalResult.builder()
                    .error( "Synchronized Materialization can only be created for source tables." )
                    .build() );
            return;
        }

        String materializedTableName;
        try {
            materializedTableName = resolveMaterializationTableName( request.getTargetEntityName(), targetNamespace.id, getNextSynchronizedMaterializationTableName( targetNamespace.id, sourceTable.name ), "Synchronized Materialization" );
        } catch ( GenericRuntimeException e ) {
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
            return;
        }
        String targetTable = quoteQualified( targetNamespace.name, materializedTableName );
        String sourceTableName = quoteQualified( sourceNamespace.name, sourceTable.name );
        List<LogicalColumn> columns = snapshot.rel().getColumns( sourceTable.id ).stream().sorted().toList();

        String createQuery = buildCreateMaterializationTableQuery( targetTable, targetStore.uniqueName, sourceTable, columns );
        Result<?, ?> createResult = executeSql( createQuery );
        if ( createResult.error != null ) {
            ctx.json( createResult );
            return;
        }

        String columnList = columns.stream()
                .map( column -> quoteIdentifier( column.name ) )
                .collect( Collectors.joining( ", " ) );
        String insertQuery = String.format( "INSERT INTO %s SELECT %s FROM %s", targetTable, columnList, sourceTableName );
        Result<?, ?> insertResult = executeSql( insertQuery );
        if ( insertResult.error != null ) {
            executeSql( "DROP TABLE " + targetTable );
            ctx.json( insertResult );
            return;
        }

        LogicalTable materializedTable = Catalog.snapshot().rel().getTable( targetNamespace.id, materializedTableName ).orElseThrow();
        Catalog.getInstance().getLogicalRel( targetNamespace.id ).setTableModifiable( materializedTable.id, false );
        Catalog.getInstance().getLogicalRel( targetNamespace.id ).setSynchronizedSourceEntity( materializedTable.id, sourceTable.id );
        Catalog.getInstance().updateSnapshot();
        List<String> foreignKeyWarnings = buildSynchronizedSourceMaterializationForeignKeyWarnings( sourceTable, materializedTableName );

        ctx.json( RelationalResult.builder()
                .table( materializedTableName )
                .namespace( targetNamespace.name )
                .query( insertQuery )
                .queryType( QueryType.DML )
                .affectedTuples( insertResult.affectedTuples )
                .changeDescriptions( foreignKeyWarnings.toArray( new String[0] ) )
                .build() );
    }


    private void refreshSynchronizedSourceMaterializationData( long materializedTableId ) {
        Snapshot snapshot = Catalog.snapshot();
        LogicalTable materializedTable = snapshot.rel().getTable( materializedTableId ).orElseThrow();
        if ( materializedTable.synchronizedSourceEntityId == null ) {
            return;
        }

        LogicalTable sourceTable = snapshot.rel().getTable( materializedTable.synchronizedSourceEntityId ).orElseThrow();
        LogicalNamespace materializedNamespace = snapshot.getNamespace( materializedTable.namespaceId ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceTable.namespaceId ).orElseThrow();
        Map<String, LogicalColumn> materializedColumns = snapshot.rel().getColumns( materializedTable.id ).stream()
                .collect( Collectors.toMap( column -> column.name, column -> column ) );
        Map<Long, LogicalColumn> sourceColumnsById = snapshot.rel().getColumns( sourceTable.id ).stream()
                .collect( Collectors.toMap( column -> column.id, column -> column ) );
        AllocationEntity sourceAllocation = snapshot.alloc().getFromLogical( sourceTable.id ).stream().findFirst().orElseThrow();
        List<LogicalColumn> columns = snapshot.alloc().getColumns( sourceAllocation.placementId ).stream()
                .map( AllocationColumn::getColumnId )
                .map( sourceColumnsById::get )
                .filter( Objects::nonNull )
                .filter( column -> materializedColumns.containsKey( column.name ) )
                .toList();
        String targetColumnList = columns.stream()
                .map( column -> quoteIdentifier( materializedColumns.get( column.name ).name ) )
                .collect( Collectors.joining( ", " ) );
        String sourceColumnList = columns.stream()
                .map( column -> quoteIdentifier( column.name ) )
                .collect( Collectors.joining( ", " ) );
        String materializedTableName = quoteQualified( materializedNamespace.name, materializedTable.name );
        String sourceTableName = quoteQualified( sourceNamespace.name, sourceTable.name );

        Catalog.getInstance().getLogicalRel( materializedNamespace.id ).setTableModifiable( materializedTable.id, true );
        Catalog.getInstance().updateSnapshot();
        try {
            Result<?, ?> deleteResult = executeSql( "DELETE FROM " + materializedTableName );
            if ( deleteResult.error != null ) {
                throw new GenericRuntimeException( deleteResult.error );
            }
            String insertQuery = String.format( "INSERT INTO %s (%s) SELECT %s FROM %s", materializedTableName, targetColumnList, sourceColumnList, sourceTableName );
            log.info( "Refreshing synchronized materialization data with query: {}", insertQuery );
            Result<?, ?> insertResult = executeSql( insertQuery );
            if ( insertResult.error != null ) {
                throw new GenericRuntimeException( insertResult.error );
            }
        } finally {
            Catalog.getInstance().getLogicalRel( materializedNamespace.id ).setTableModifiable( materializedTable.id, false );
            Catalog.getInstance().updateSnapshot();
        }
    }


    private List<String> buildSynchronizedSourceMaterializationForeignKeyWarnings( LogicalTable sourceTable, String materializedTableName ) {
        Snapshot snapshot = Catalog.snapshot();
        AllocationEntity sourceAllocation = snapshot.alloc().getFromLogical( sourceTable.id ).stream()
                .findFirst()
                .orElse( null );
        if ( sourceAllocation == null ) {
            return List.of();
        }

        AdapterCatalog sourceAdapterCatalog = Catalog.getInstance().getAdapterCatalog( sourceAllocation.adapterId ).orElse( null );
        DataSource<?> sourceAdapter = AdapterManager.getInstance().getSource( sourceAllocation.adapterId ).orElse( null );
        if ( sourceAdapterCatalog == null || sourceAdapter == null ) {
            return List.of();
        }

        PhysicalTable sourcePhysicalTable = sourceAdapterCatalog.getPhysicalsFromAllocs( sourceAllocation.id ).stream()
                .findFirst()
                .flatMap( physical -> physical.unwrap( PhysicalTable.class ) )
                .orElse( null );
        if ( sourcePhysicalTable == null ) {
            return List.of();
        }

        Map<Long, LogicalTable> synchronizedTablesBySourceId = snapshot.rel().getTables(
                        (org.polypheny.db.catalog.logistic.Pattern) null,
                        (org.polypheny.db.catalog.logistic.Pattern) null ).stream()
                .filter( table -> table.synchronizedSourceEntityId != null )
                .collect( Collectors.toMap( table -> table.synchronizedSourceEntityId, table -> table, ( left, right ) -> left ) );

        Map<String, LogicalTable> sourceTablesByPhysicalName = getSourceTablesByPhysicalName( snapshot, sourceAdapterCatalog, sourceAllocation.adapterId );
        return sourceAdapter.asRelationalDataSource()
                .getExportedForeignKeysForTable( sourcePhysicalTable.namespaceName, sourcePhysicalTable.name ).stream()
                .map( foreignKey -> formatSynchronizedSourceMaterializationForeignKeyWarning( foreignKey, materializedTableName, sourceTablesByPhysicalName, synchronizedTablesBySourceId ) )
                .filter( Objects::nonNull )
                .sorted()
                .toList();
    }


    private String formatSynchronizedSourceMaterializationForeignKeyWarning(
            ExportedForeignKey foreignKey,
            String materializedTableName,
            Map<String, LogicalTable> sourceTablesByPhysicalName,
            Map<Long, LogicalTable> synchronizedTablesBySourceId ) {
        LogicalTable referencedSourceTable = sourceTablesByPhysicalName.get( toPhysicalTableKey( foreignKey.referencedPhysicalSchemaName(), foreignKey.referencedPhysicalTableName() ) );
        if ( referencedSourceTable != null && synchronizedTablesBySourceId.containsKey( referencedSourceTable.id ) ) {
            return null;
        }

        String referencedSourceTableName = referencedSourceTable == null
                ? foreignKey.referencedPhysicalTableName()
                : referencedSourceTable.name;
        return "Foreign key " + foreignKey.name()
                + " references source table " + referencedSourceTableName
                + " and cannot be preserved. Materialize " + referencedSourceTableName
                + " first, then refresh " + materializedTableName
                + " to add the foreign key.";
    }


    private Map<String, LogicalTable> getSourceTablesByPhysicalName( Snapshot snapshot, AdapterCatalog sourceAdapterCatalog, long sourceAdapterId ) {
        Map<String, LogicalTable> tablesByPhysicalName = new HashMap<>();
        for ( AllocationEntity allocation : snapshot.alloc().getAllocations().stream().filter( allocation -> allocation.adapterId == sourceAdapterId ).toList() ) {
            LogicalTable logicalTable = snapshot.rel().getTable( allocation.logicalId ).orElse( null );
            if ( logicalTable == null || logicalTable.entityType != EntityType.SOURCE ) {
                continue;
            }

            for ( PhysicalEntity physicalEntity : sourceAdapterCatalog.getPhysicalsFromAllocs( allocation.id ) ) {
                physicalEntity.unwrap( PhysicalTable.class ).ifPresent( physicalTable -> tablesByPhysicalName.put(
                        toPhysicalTableKey( physicalTable.namespaceName, physicalTable.name ),
                        logicalTable ) );
            }
        }
        return tablesByPhysicalName;
    }


    private static String toPhysicalTableKey( String schemaName, String tableName ) {
        return normalizePhysicalName( schemaName ) + "." + normalizePhysicalName( tableName );
    }


    private static String normalizePhysicalName( String name ) {
        return name == null ? "" : name.toLowerCase();
    }


    void createIndependentSourceCollectionMaterialization( final Context ctx ) {
        SourceMaterializationRequest request = ctx.bodyAsClass( SourceMaterializationRequest.class );
        Snapshot snapshot = Catalog.snapshot();
        LogicalCollection sourceCollection = snapshot.doc().getCollection( request.getSourceEntityId() ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceCollection.namespaceId ).orElseThrow();
        long targetNamespaceId = request.getTargetNamespaceId() == null ? sourceNamespace.id : request.getTargetNamespaceId();
        LogicalNamespace targetNamespace = snapshot.getNamespace( targetNamespaceId ).orElseThrow();
        LogicalAdapter targetStore = snapshot.getAdapter( request.getTargetStoreId() ).orElseThrow();

        String independentCollectionName;
        try {
            independentCollectionName = resolveMaterializationCollectionName( request.getTargetEntityName(), targetNamespace.id, getNextIndependentMaterializationCollectionName( targetNamespace.id, sourceCollection.name ), "Independent Materialization" );
        } catch ( GenericRuntimeException e ) {
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
            return;
        }
        String createQuery = buildCreateMaterializationCollectionQuery( independentCollectionName, targetStore.uniqueName );
        Result<?, ?> createResult = executeMql( createQuery, targetNamespace.name, false );
        if ( createResult.error != null ) {
            ctx.json( createResult );
            return;
        }

        long copiedDocuments;
        try {
            copiedDocuments = copyCollectionDocuments( sourceCollection, sourceNamespace.name, independentCollectionName, targetNamespace.name );
        } catch ( GenericRuntimeException e ) {
            executeMql( "db." + independentCollectionName + ".drop()", targetNamespace.name, false );
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
            return;
        }

        ctx.json( RelationalResult.builder()
                .dataModel( DataModel.DOCUMENT )
                .table( independentCollectionName )
                .namespace( targetNamespace.name )
                .query( buildBatchedCopyQueryDescription( sourceCollection.name, independentCollectionName ) )
                .queryType( QueryType.DML )
                .affectedTuples( copiedDocuments )
                .build() );
    }


    void createSynchronizedSourceCollectionMaterialization( final Context ctx ) {
        SourceMaterializationRequest request = ctx.bodyAsClass( SourceMaterializationRequest.class );
        Snapshot snapshot = Catalog.snapshot();
        LogicalCollection sourceCollection = snapshot.doc().getCollection( request.getSourceEntityId() ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceCollection.namespaceId ).orElseThrow();
        long targetNamespaceId = request.getTargetNamespaceId() == null ? sourceNamespace.id : request.getTargetNamespaceId();
        LogicalNamespace targetNamespace = snapshot.getNamespace( targetNamespaceId ).orElseThrow();
        LogicalAdapter targetStore = snapshot.getAdapter( request.getTargetStoreId() ).orElseThrow();

        String synchronizedCollectionName;
        try {
            synchronizedCollectionName = resolveMaterializationCollectionName( request.getTargetEntityName(), targetNamespace.id, getNextSynchronizedMaterializationCollectionName( targetNamespace.id, sourceCollection.name ), "Synchronized Materialization" );
        } catch ( GenericRuntimeException e ) {
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
            return;
        }
        String createQuery = buildCreateMaterializationCollectionQuery( synchronizedCollectionName, targetStore.uniqueName );
        Result<?, ?> createResult = executeMql( createQuery, targetNamespace.name, false );
        if ( createResult.error != null ) {
            ctx.json( createResult );
            return;
        }

        long copiedDocuments;
        try {
            copiedDocuments = copyCollectionDocuments( sourceCollection, sourceNamespace.name, synchronizedCollectionName, targetNamespace.name );
        } catch ( GenericRuntimeException e ) {
            executeMql( "db." + synchronizedCollectionName + ".drop()", targetNamespace.name, false );
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
            return;
        }

        setSynchronizedCollectionMetadata( targetNamespace.id, synchronizedCollectionName, sourceCollection.id );

        ctx.json( RelationalResult.builder()
                .dataModel( DataModel.DOCUMENT )
                .table( synchronizedCollectionName )
                .namespace( targetNamespace.name )
                .query( buildBatchedCopyQueryDescription( sourceCollection.name, synchronizedCollectionName ) )
                .queryType( QueryType.DML )
                .affectedTuples( copiedDocuments )
                .build() );
    }


    void dropSynchronizedSourceMaterialization( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        Transaction transaction = getTransaction();
        boolean committed = false;
        try {
            Statement statement = transaction.createStatement();
            Snapshot snapshot = Catalog.snapshot();
            Optional<LogicalTable> table = snapshot.rel().getTable( request.entityId );
            if ( table.isPresent() ) {
                if ( table.get().synchronizedSourceEntityId == null ) {
                    transaction.rollback( "Selected table is not a synchronized materialization." );
                    ctx.json( RelationalResult.builder().error( "Selected table is not a synchronized materialization." ).build() );
                    return;
                }
                DdlManager.getInstance().dropTable( table.get(), statement );
                transaction.commit();
                committed = true;
                ctx.json( RelationalResult.builder().build() );
                return;
            }

            Optional<LogicalCollection> collection = snapshot.doc().getCollection( request.entityId );
            if ( collection.isPresent() ) {
                if ( collection.get().synchronizedSourceEntityId == null ) {
                    transaction.rollback( "Selected collection is not a synchronized materialization." );
                    ctx.json( RelationalResult.builder().error( "Selected collection is not a synchronized materialization." ).build() );
                    return;
                }
                DdlManager.getInstance().dropCollection( collection.get(), statement );
                transaction.commit();
                committed = true;
                ctx.json( RelationalResult.builder().build() );
                return;
            }

            transaction.rollback( "Selected entity does not exist." );
            ctx.json( RelationalResult.builder().error( "Selected entity does not exist." ).build() );
        } catch ( Exception e ) {
            if ( !committed ) {
                try {
                    transaction.rollback( "Error while dropping synchronized materialization: " + e.getMessage() );
                } catch ( Exception rollbackException ) {
                    log.error( "Rollback also failed", rollbackException );
                }
            }
            ctx.json( RelationalResult.builder().error( "Could not drop synchronized materialization: " + e.getMessage() ).build() );
        }
    }


    private static void setSynchronizedCollectionMetadata( long namespaceId, String collectionName, long sourceCollectionId ) {
        LogicalCollection collection = Catalog.snapshot().doc().getCollection( namespaceId, collectionName ).orElseThrow();
        Catalog.getInstance().getLogicalDoc( namespaceId ).setCollectionModifiable( collection.id, false );
        Catalog.getInstance().getLogicalDoc( namespaceId ).setSynchronizedSourceEntity( collection.id, sourceCollectionId );
        Catalog.getInstance().updateSnapshot();
    }


    public SourceMaterializationRefreshResult refreshSynchronizedSourceCollectionMaterializationData( UIRequest request ) {
        LogicalCollection materializedCollection = Catalog.snapshot().doc().getCollection( request.entityId ).orElseThrow();
        if ( materializedCollection.synchronizedSourceEntityId == null ) {
            return new SourceMaterializationRefreshResult( List.of(), null, false );
        }

        List<String> sourceRefreshPreview = DdlManager.getInstance().previewSourceCollectionRefresh( materializedCollection.synchronizedSourceEntityId );
        if ( hasSourceDeletedChange( sourceRefreshPreview ) ) {
            return new SourceMaterializationRefreshResult( sourceRefreshPreview, null, true );
        }

        long documentCount = countSynchronizedSourceCollectionMaterializationDocuments( materializedCollection.id );
        if ( !request.confirmedDataRefresh ) {
            return new SourceMaterializationRefreshResult( List.of(), documentCount, false );
        }

        refreshSynchronizedSourceCollectionMaterializationData( materializedCollection.id );
        return new SourceMaterializationRefreshResult( List.of(), null, false );
    }


    private long countSynchronizedSourceCollectionMaterializationDocuments( long materializedCollectionId ) {
        Snapshot snapshot = Catalog.snapshot();
        LogicalCollection materializedCollection = snapshot.doc().getCollection( materializedCollectionId ).orElseThrow();
        if ( materializedCollection.synchronizedSourceEntityId == null ) {
            return 0;
        }
        LogicalCollection sourceCollection = snapshot.doc().getCollection( materializedCollection.synchronizedSourceEntityId ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceCollection.namespaceId ).orElseThrow();
        Result<?, ?> countResult = executeMql( "db." + sourceCollection.name + ".countDocuments({})", sourceNamespace.name, true );
        if ( countResult.error != null ) {
            throw new GenericRuntimeException( countResult.error );
        }
        if ( countResult.data == null || countResult.data.length == 0 ) {
            return 0;
        }
        Matcher matcher = Pattern.compile( "-?\\d+" ).matcher( String.valueOf( countResult.data[0] ) );
        if ( !matcher.find() ) {
            throw new GenericRuntimeException( "Could not determine document count for source collection " + sourceCollection.name + "." );
        }
        return Long.parseLong( matcher.group() );
    }


    private void refreshSynchronizedSourceCollectionMaterializationData( long materializedCollectionId ) {
        Snapshot snapshot = Catalog.snapshot();
        LogicalCollection materializedCollection = snapshot.doc().getCollection( materializedCollectionId ).orElseThrow();
        if ( materializedCollection.synchronizedSourceEntityId == null ) {
            return;
        }

        LogicalCollection sourceCollection = snapshot.doc().getCollection( materializedCollection.synchronizedSourceEntityId ).orElseThrow();
        LogicalNamespace materializedNamespace = snapshot.getNamespace( materializedCollection.namespaceId ).orElseThrow();
        LogicalNamespace sourceNamespace = snapshot.getNamespace( sourceCollection.namespaceId ).orElseThrow();
        Catalog.getInstance().getLogicalDoc( materializedNamespace.id ).setCollectionModifiable( materializedCollection.id, true );
        Catalog.getInstance().updateSnapshot();
        try {
            Result<?, ?> deleteResult = executeMql( "db." + materializedCollection.name + ".deleteMany({})", materializedNamespace.name, false );
            if ( deleteResult.error != null ) {
                throw new GenericRuntimeException( deleteResult.error );
            }
            copyCollectionDocuments( sourceCollection, sourceNamespace.name, materializedCollection.name, materializedNamespace.name );
        } finally {
            Catalog.getInstance().getLogicalDoc( materializedNamespace.id ).setCollectionModifiable( materializedCollection.id, false );
            Catalog.getInstance().updateSnapshot();
        }
    }


    private long copyCollectionDocuments( LogicalCollection sourceCollection, String sourceNamespace, String targetCollectionName, String targetNamespace ) {
        Transaction transaction = getTransaction();
        ImplementationContext implementationContext = null;
        ResultIterator iterator = null;
        boolean committed = false;
        try {
            implementationContext = LanguageManager.getINSTANCE().anyPrepareQuery(
                    QueryContext.builder()
                            .query( String.format( "db.%s.find({})", sourceCollection.name ) )
                            .language( QueryLanguage.from( "mql" ) )
                            .origin( ORIGIN )
                            .namespaceId( LanguageCrud.getNamespaceIdOrDefault( sourceNamespace ) )
                            .batch( DOCUMENT_MATERIALIZATION_COPY_BATCH_SIZE )
                            .transactions( List.of( transaction ) )
                            .transactionManager( transactionManager )
                            .build(), transaction ).get( 0 );
            ExecutedContext executedContext = implementationContext.execute( implementationContext.getStatement() );
            if ( executedContext.getException().isPresent() ) {
                throw new GenericRuntimeException( executedContext.getException().get().getMessage() );
            }

            iterator = executedContext.getIterator();
            long copiedDocuments = 0;
            while ( true ) {
                List<List<PolyValue>> batch = iterator.getNextBatch( DOCUMENT_MATERIALIZATION_COPY_BATCH_SIZE );
                if ( batch.isEmpty() ) {
                    transaction.commit();
                    committed = true;
                    return copiedDocuments;
                }

                String[] documents = batch.stream()
                        .map( row -> row.get( 0 ).toJson() )
                        .toArray( String[]::new );
                Result<?, ?> insertResult = executeMql( buildInsertManyQuery( targetCollectionName, documents ), targetNamespace, false );
                if ( insertResult.error != null ) {
                    throw new GenericRuntimeException( insertResult.error );
                }
                copiedDocuments += documents.length;
            }
        } catch ( Exception e ) {
            if ( !committed ) {
                transaction.rollback( "Error while copying source collection documents: " + e.getMessage() );
            }
            if ( e instanceof GenericRuntimeException ) {
                throw (GenericRuntimeException) e;
            }
            throw new GenericRuntimeException( e );
        } finally {
            if ( iterator != null ) {
                iterator.close();
            }
        }
    }


    private static String buildBatchedCopyQueryDescription( String sourceCollectionName, String targetCollectionName ) {
        return String.format( "db.%s.find({}) -> db.%s.insertMany(...) in batches", sourceCollectionName, targetCollectionName );
    }


    private Result<?, ?> copyRelationalTableRows( LogicalTable sourceTable, String sourceTableName, String targetTable, List<LogicalColumn> columns ) {
        String columnList = columns.stream()
                .map( column -> quoteIdentifier( column.name ) )
                .collect( Collectors.joining( ", " ) );
        String orderBy = buildMaterializationCopyOrderBy( sourceTable, columns );
        long rowCount = countRelationalTableRows( sourceTableName );
        long copiedRows = 0;
        for ( long offset = 0; offset < rowCount; offset += RELATIONAL_MATERIALIZATION_COPY_BATCH_SIZE ) {
            String insertQuery = String.format(
                    "INSERT INTO %s (%s) SELECT %s FROM %s%s LIMIT %d OFFSET %d",
                    targetTable,
                    columnList,
                    columnList,
                    sourceTableName,
                    orderBy,
                    RELATIONAL_MATERIALIZATION_COPY_BATCH_SIZE,
                    offset );
            Result<?, ?> insertResult = executeSql( insertQuery );
            if ( insertResult.error != null ) {
                return insertResult;
            }
            copiedRows += insertResult.affectedTuples;
        }
        return RelationalResult.builder()
                .query( buildBatchedRelationalCopyQueryDescription( targetTable, sourceTableName ) )
                .queryType( QueryType.DML )
                .affectedTuples( copiedRows )
                .build();
    }


    private long countRelationalTableRows( String tableName ) {
        RelationalResult countResult = (RelationalResult) executeSql( "SELECT COUNT(*) FROM " + tableName );
        if ( countResult.error != null ) {
            throw new GenericRuntimeException( countResult.error );
        }
        if ( countResult.data == null || countResult.data.length == 0 || countResult.data[0].length == 0 ) {
            return 0;
        }
        return Long.parseLong( countResult.data[0][0] );
    }


    private static String buildMaterializationCopyOrderBy( LogicalTable sourceTable, List<LogicalColumn> columns ) {
        if ( sourceTable.primaryKey == null ) {
            return "";
        }
        LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( sourceTable.primaryKey ).orElse( null );
        if ( primaryKey == null || primaryKey.fieldIds.isEmpty() ) {
            return "";
        }
        Map<Long, String> columnNames = columns.stream()
                .collect( Collectors.toMap( column -> column.id, column -> column.name ) );
        String orderBy = primaryKey.fieldIds.stream()
                .map( columnNames::get )
                .filter( Objects::nonNull )
                .map( Crud::quoteIdentifier )
                .collect( Collectors.joining( ", " ) );
        return orderBy.isEmpty() ? "" : " ORDER BY " + orderBy;
    }


    private static String buildBatchedRelationalCopyQueryDescription( String targetTable, String sourceTableName ) {
        return String.format( "INSERT INTO %s SELECT ... FROM %s in batches", targetTable, sourceTableName );
    }


    private Result<?, ?> executeSql( String query ) {
        return LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( "sql" ) )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
    }


    private Result<?, ?> executeMql( String query, String namespace, boolean noLimit ) {
        return LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( "mql" ) )
                        .origin( ORIGIN )
                        .namespaceId( LanguageCrud.getNamespaceIdOrDefault( namespace ) )
                        .batch( noLimit ? -1 : getPageSize() )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder()
                        .namespace( namespace )
                        .noLimit( noLimit )
                        .build() ).get( 0 );
    }


    private String buildCreateMaterializationTableQuery( String targetTable, String targetStoreName, LogicalTable sourceTable, List<LogicalColumn> columns ) {
        StringJoiner columnJoiner = new StringJoiner( ", " );
        for ( LogicalColumn column : columns ) {
            columnJoiner.add( buildMaterializationColumnDefinition( column ) );
        }

        if ( sourceTable.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( sourceTable.primaryKey ).orElse( null );
            if ( primaryKey != null ) {
                String primaryKeyColumns = primaryKey.getFieldNames().stream()
                        .map( Crud::quoteIdentifier )
                        .collect( Collectors.joining( ", " ) );
                columnJoiner.add( "PRIMARY KEY (" + primaryKeyColumns + ")" );
            }
        }

        return String.format( "CREATE TABLE %s (%s) ON STORE %s", targetTable, columnJoiner, quoteIdentifier( targetStoreName ) );
    }


    private static String buildMaterializationColumnDefinition( LogicalColumn column ) {
        StringBuilder builder = new StringBuilder();
        builder.append( quoteIdentifier( column.name ) ).append( " " ).append( buildMaterializationColumnType( column ) );
        if ( !column.nullable ) {
            builder.append( " NOT NULL" );
        }
        return builder.toString();
    }


    private static String buildMaterializationColumnType( LogicalColumn column ) {
        StringBuilder builder = new StringBuilder( column.type.getName() );
        if ( column.length != null && column.scale != null && column.type.allowsPrecScale( true, true ) ) {
            builder.append( "(" ).append( column.length ).append( ", " ).append( column.scale ).append( ")" );
        } else if ( column.length != null && column.type.allowsPrecNoScale() ) {
            builder.append( "(" ).append( getMaterializationColumnLength( column ) ).append( ")" );
        }

        if ( isMaterializationCollectionType( column.collectionsType ) ) {
            builder.append( " " ).append( column.collectionsType.getName() );
            if ( column.dimension != null ) {
                builder.append( "(" ).append( column.dimension );
                if ( column.cardinality != null ) {
                    builder.append( ", " ).append( column.cardinality );
                }
                builder.append( ")" );
            }
        }
        return builder.toString();
    }


    private static int getMaterializationColumnLength( LogicalColumn column ) {
        if ( column.type == PolyType.VARCHAR && column.length > POSTGRES_MAX_VARCHAR_LENGTH ) {
            return POSTGRES_MAX_VARCHAR_LENGTH;
        }
        return column.length;
    }


    private static boolean isMaterializationCollectionType( PolyType collectionsType ) {
        return collectionsType == PolyType.ARRAY || collectionsType == PolyType.MAP;
    }


    private static String resolveMaterializationTableName( String requestedName, long namespaceId, String generatedName, String materializationType ) {
        String targetName = normalizeRequestedMaterializationName( requestedName );
        if ( targetName == null ) {
            return generatedName;
        }
        if ( Catalog.snapshot().rel().getTable( namespaceId, targetName ).isPresent() ) {
            throw new GenericRuntimeException( materializationType + " target table '" + targetName + "' already exists." );
        }
        return targetName;
    }


    private static String resolveMaterializationCollectionName( String requestedName, long namespaceId, String generatedName, String materializationType ) {
        String targetName = normalizeRequestedMaterializationName( requestedName );
        if ( targetName == null ) {
            return generatedName;
        }
        if ( Catalog.snapshot().doc().getCollection( namespaceId, targetName ).isPresent() ) {
            throw new GenericRuntimeException( materializationType + " target collection '" + targetName + "' already exists." );
        }
        return targetName;
    }


    private static String normalizeRequestedMaterializationName( String requestedName ) {
        if ( requestedName == null || requestedName.trim().isEmpty() ) {
            return null;
        }
        return requestedName.trim();
    }


    private static String getNextIndependentMaterializationTableName( long namespaceId, String sourceTableName ) {
        String baseName = sourceTableName + "_independent";
        String candidate = baseName;
        int suffix = 2;
        while ( Catalog.snapshot().rel().getTable( namespaceId, candidate ).isPresent() ) {
            candidate = baseName + suffix;
            suffix++;
        }
        return candidate;
    }


    private static String getNextSynchronizedMaterializationTableName( long namespaceId, String sourceTableName ) {
        String baseName = sourceTableName + "_synchronized";
        String candidate = baseName;
        int suffix = 2;
        while ( Catalog.snapshot().rel().getTable( namespaceId, candidate ).isPresent() ) {
            candidate = baseName + suffix;
            suffix++;
        }
        return candidate;
    }


    private static String getNextIndependentMaterializationCollectionName( long namespaceId, String sourceCollectionName ) {
        String baseName = sourceCollectionName + "_independent";
        String candidate = baseName;
        int suffix = 2;
        while ( Catalog.snapshot().doc().getCollection( namespaceId, candidate ).isPresent() ) {
            candidate = baseName + suffix;
            suffix++;
        }
        return candidate;
    }


    private static String getNextSynchronizedMaterializationCollectionName( long namespaceId, String sourceCollectionName ) {
        String baseName = sourceCollectionName + "_synchronized";
        String candidate = baseName;
        int suffix = 2;
        while ( Catalog.snapshot().doc().getCollection( namespaceId, candidate ).isPresent() ) {
            candidate = baseName + suffix;
            suffix++;
        }
        return candidate;
    }


    private static String buildCreateMaterializationCollectionQuery( String targetCollectionName, String targetStoreName ) {
        return String.format( "db.createCollection(\"%s\").store(\"%s\")", targetCollectionName, targetStoreName );
    }


    private static String buildInsertManyQuery( String targetCollectionName, String[] documents ) {
        return String.format( "db.%s.insertMany([%s])", targetCollectionName, String.join( ",", documents ) );
    }


    private static String quoteQualified( String namespaceName, String entityName ) {
        return quoteIdentifier( namespaceName ) + "." + quoteIdentifier( entityName );
    }


    private static String quoteIdentifier( String identifier ) {
        return "\"" + identifier.replace( "\"", "\"\"" ) + "\"";
    }


    /**
     * Initialize a multipart request, so that the values can be fetched with request.raw().getPart( name )
     */
    private void initMultipart( final Context ctx ) {
        //see https://stackoverflow.com/questions/34746900/sparkjava-upload-file-didt-work-in-spark-java-framework
        String location = System.getProperty( "java.io.tmpdir" + File.separator + "Polypheny-DB" );
        long maxSizeMB = RuntimeConfig.UI_UPLOAD_SIZE_MB.getInteger();
        long maxFileSize = 1_000_000L * maxSizeMB;
        long maxRequestSize = 1_000_000L * maxSizeMB;
        int fileSizeThreshold = 1024;
        MultipartConfigElement multipartConfigElement = new MultipartConfigElement( location, maxFileSize, maxRequestSize, fileSizeThreshold );
        ctx.attribute( "org.eclipse.jetty.multipartConfig", multipartConfigElement );
    }


    /**
     * Insert data into a table
     */
    void insertTuple( final Context ctx ) throws IOException {
        ctx.contentType( "multipart/form-data" );
        initMultipart( ctx );
        String unparsed = ctx.formParam( "entityId" );
        if ( unparsed == null ) {
            throw new GenericRuntimeException( "Error on tuple insert" );
        }

        long entityId = Long.parseLong( unparsed );

        LogicalTable table = Catalog.snapshot().rel().getTable( entityId ).orElseThrow();
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( table.namespaceId ).orElseThrow();
        String entityName = String.format( "\"%s\".\"%s\"", namespace.name, table.name );

        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StringJoiner columns = new StringJoiner( ",", "(", ")" );
        StringJoiner values = new StringJoiner( ",", "(", ")" );

        List<LogicalColumn> logicalColumns = Catalog.snapshot().rel().getColumns( table.id );
        try {
            int i = 0;
            for ( LogicalColumn logicalColumn : logicalColumns ) {
                //part is null if it does not exist
                Part part = ctx.req().getPart( logicalColumn.name );
                if ( part == null ) {
                    //don't add if default value is set
                    if ( logicalColumn.defaultValue == null ) {
                        values.add( "NULL" );
                        columns.add( "\"" + logicalColumn.name + "\"" );
                    }
                } else {
                    columns.add( "\"" + logicalColumn.name + "\"" );
                    if ( part.getSubmittedFileName() == null ) {
                        String value = new BufferedReader( new InputStreamReader( part.getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
                        if ( logicalColumn.name.equals( "_id" ) ) {
                            if ( value.isEmpty() ) {
                                value = BsonUtil.getObjectId();
                            }
                        }
                        values.add( uiValueToSql( value, logicalColumn.type, logicalColumn.collectionsType ) );
                    } else {
                        values.add( "?" );
                        FileInputHandle fih = new FileInputHandle( statement, part.getInputStream() );
                        statement.getDataContext().addParameterValues( i++, logicalColumn.getAlgDataType( transaction.getTypeFactory() ), ImmutableList.of( PolyBlob.of( fih.getData() ) ) );
                    }
                }
            }
        } catch ( ServletException e ) {
            throw new GenericRuntimeException( e );
        }

        String query = String.format( "INSERT INTO %s %s VALUES %s", entityName, columns, values );
        QueryLanguage language = QueryLanguage.from( "sql" );
        QueryContext context = QueryContext.builder()
                .query( query )
                .language( language )
                .origin( ORIGIN )
                .statement( statement )
                .transactions( new ArrayList<>( List.of( transaction ) ) )
                .transactionManager( transactionManager )
                .build();

        UIRequest request = UIRequest.builder().build();
        Result<?, ?> result = LanguageCrud.anyQueryResult( context, request ).get( 0 );
        ctx.json( result );

    }

    /**
     * Run any query coming from the SQL console
     */
    /*public static List<RelationalResult> anySqlQuery( final QueryRequest request, final Session session, Crud crud ) {
        Transaction transaction = getTransaction( request.analyze, request.cache, crud );

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        List<RelationalResult> results = new ArrayList<>();
        boolean autoCommit = true;

        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( request.analyze ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( crud );
        }

        // TODO: make it possible to use pagination
        String[] queries;
        try {
            queries = transaction.getProcessor( QueryLanguage.from( "sql" ) ).splitStatements( request.query ).toArray( new String[0] );
        } catch ( RuntimeException e ) {
            return List.of( RelationalResult.builder().error( "Syntax error: " + e.getMessage() ).build() );
        }

        // No autoCommit if the query has commits.
        // Ignore case: from: https://alvinalexander.com/blog/post/java/java-how-case-insensitive-search-string-matches-method
        Pattern p = Pattern.compile( ".*(COMMIT|ROLLBACK).*", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
        for ( String query : queries ) {
            if ( p.matcher( query ).matches() ) {
                autoCommit = false;
                break;
            }
        }
        long executionTime = 0;
        long temp = 0;
        boolean noLimit;
        for ( String query : queries ) {
            RelationalResult result;
            if ( !transaction.isActive() ) {
                transaction = getTransaction( request.analyze, request.cache, crud );
            }
            if ( Pattern.matches( "(?si:[\\s]*COMMIT.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    transaction.commit();
                    executionTime += System.nanoTime() - temp;
                    transaction = getTransaction( request.analyze, request.cache, crud );
                    results.add( RelationalResult.builder().query( query ).build() );
                } catch ( TransactionException e ) {
                    log.error( "Caught exception while committing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    log.error( e.toString() );
                }
            } else if ( Pattern.matches( "(?si:[\\s]*ROLLBACK.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    transaction.rollback();
                    executionTime += System.nanoTime() - temp;
                    transaction = getTransaction( request.analyze, request.cache, crud );
                    results.add( RelationalResult.builder().query( query ).build() );
                } catch ( TransactionException e ) {
                    log.error( "Caught exception while rolling back a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                }
            } else if ( Pattern.matches( "(?si:^[\\s]*[/(\\s]*SELECT.*)", query ) ) {
                // Add limit if not specified
                Pattern p2 = Pattern.compile( "(?si:limit)[\\s]+[0-9]+[\\s]*$" );
                //If the user specifies a limit
                noLimit = p2.matcher( query ).find() || request.noLimit;
                try {
                    temp = System.nanoTime();
                    result = executeSqlSelect( transaction.createStatement(), request, query, noLimit, crud )
                            .query( query )
                            .xid( transaction.getXid().toString() ).build();
                    executionTime += System.nanoTime() - temp;
                    results.add( result );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = Crud.getTransaction( request.analyze, request.cache, crud );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    if ( e.getCause() instanceof AvaticaRuntimeException ) {
                        result = RelationalResult.builder().error( ((AvaticaRuntimeException) e.getCause()).getErrorMessage() ).build();
                    } else {
                        result = RelationalResult.builder().error( e.getCause().getMessage() ).build();
                    }
                    results.add( result.toBuilder().query( query ).xid( transaction.getXid().toString() ).build() );
                    try {
                        transaction.rollback();
                    } catch ( TransactionException ex ) {
                        log.error( "Caught exception while rollback", e );
                    }
                }
            } else {
                try {
                    temp = System.nanoTime();
                    int numOfRows = crud.executeSqlUpdate( transaction, query );
                    executionTime += System.nanoTime() - temp;

                    results.add( RelationalResult.builder().affectedTuples( numOfRows ).query( query ).xid( transaction.getXid().toString() ).build() );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze, request.cache, crud );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    results.add( RelationalResult.builder().error( e.getMessage() ).query( query ).xid( transaction.getXid().toString() ).build() );
                    try {
                        transaction.rollback();
                    } catch ( TransactionException ex ) {
                        log.error( "Caught exception while rollback", e );
                    }
                }
            }

        }

        String commitStatus;
        try {
            transaction.commit();
            commitStatus = "Committed";
        } catch ( TransactionException e ) {
            log.error( "Caught exception", e );
            results.add( RelationalResult.builder().error( e.getMessage() ).build() );
            try {
                transaction.rollback();
                commitStatus = "Rolled back";
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
                commitStatus = "Error while rolling back";
            }
        }

        if ( queryAnalyzer != null ) {
            attachQueryAnalyzer( queryAnalyzer, executionTime, commitStatus, results.size() );
        }

        return results;
    }*/


    /**
     * Converts a String, such as "'12:00:00'" into a valid SQL statement, such as "TIME '12:00:00'"
     */
    public static String uiValueToSql( final String value, final PolyType type, final PolyType collectionsType ) {
        if ( value == null ) {
            return "NULL";
        }
        if ( collectionsType == PolyType.ARRAY ) {
            return "ARRAY " + value;
        }
        switch ( type ) {
            case TIME:
                return String.format( "TIME '%s'", value );
            case DATE:
                return String.format( "DATE '%s'", value );
            case TIMESTAMP:
                return String.format( "TIMESTAMP '%s'", value );
        }
        if ( type.getFamily() == PolyTypeFamily.CHARACTER ) {
            return String.format( "'%s'", value );
        }
        return value;
    }


    /**
     * Compute a WHERE condition from a filter that only consists of the PK column WHERE clauses
     * There WHERE clause contains a space at the beginning, for convenience
     *
     * @param filter Filter. Key: column name, value: the value of the entry, e.g. 1 or abc or [1,2,3] or {@code null}
     */
    private String computeWherePK( final LogicalTable table, final Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", "", "" );
        Map<Long, LogicalColumn> columns = Catalog.snapshot().rel().getColumns( table.id ).stream().collect( Collectors.toMap( c -> c.id, c -> c ) );
        if ( columns.isEmpty() ) {
            throw new GenericRuntimeException( "Table has no columns" );
        }

        LogicalPrimaryKey pk = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
        for ( long colId : pk.fieldIds ) {
            LogicalColumn col = columns.get( colId );
            String condition;
            if ( filter.containsKey( col.name ) ) {
                String val = filter.get( col.name );

                condition = uiValueToSql( val, col.type, col.collectionsType );
                condition = String.format( "\"%s\" = %s", col.name, condition );
                joiner.add( condition );
            }
        }
        return " WHERE " + joiner;
    }


    /**
     * Delete a row from a table. The row is determined by the value of every PK column in that row (conjunction).
     */
    void deleteTuple( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        StringBuilder query = new StringBuilder();

        String tableId = getFullEntityName( request.entityId );
        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();

        query.append( "DELETE FROM " ).append( tableId ).append( computeWherePK( table, request.data ) );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> result = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query.toString() )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );

        ctx.json( result );
    }


    /**
     * Update a row from a table. The row is determined by the value of every PK column in that row (conjunction).
     */
    void updateTuple( final Context ctx ) throws ServletException, IOException {
        ctx.contentType( "multipart/form-data" );
        initMultipart( ctx );
        Map<String, String> oldValues = null;
        long entityId = Long.parseLong( Objects.requireNonNull( ctx.formParam( "entityId" ) ) );
        try {
            String _oldValues = new BufferedReader( new InputStreamReader( ctx.req().getPart( "oldValues" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
            oldValues = gson.fromJson( _oldValues, Map.class );
        } catch ( IOException | ServletException e ) {
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
        }
        String fullName = getFullEntityName( entityId );

        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StringJoiner setStatements = new StringJoiner( ",", "", "" );

        List<LogicalColumn> logicalColumns = Catalog.snapshot().rel().getColumns( entityId );

        int i = 0;
        for ( LogicalColumn logicalColumn : logicalColumns ) {
            Part part = ctx.req().getPart( logicalColumn.name );
            if ( part == null ) {
                continue;
            }
            if ( part.getSubmittedFileName() == null ) {
                String value = new BufferedReader( new InputStreamReader( part.getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
                String parsed = gson.fromJson( value, String.class );
                if ( parsed == null ) {
                    setStatements.add( String.format( "\"%s\" = NULL", logicalColumn.name ) );
                } else {
                    setStatements.add( String.format( "\"%s\" = %s", logicalColumn.name, uiValueToSql( parsed, logicalColumn.type, logicalColumn.collectionsType ) ) );
                }
            } else {
                setStatements.add( String.format( "\"%s\" = ?", logicalColumn.name ) );
                FileInputHandle fih = new FileInputHandle( statement, part.getInputStream() );
                statement.getDataContext().addParameterValues( i++, logicalColumn.getAlgDataType( transaction.getTypeFactory() ), ImmutableList.of( PolyBlob.of( fih.getData() ) ) );
            }
        }

        String query = "UPDATE "
                + fullName
                + " SET "
                + setStatements
                + computeWherePK( Catalog.snapshot().rel().getTable( logicalColumns.get( 0 ).tableId ).orElseThrow(), oldValues );

        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> result = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .statement( statement )
                        .transactions( List.of( transaction ) )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );

        ctx.json( result );
    }


    void batchUpdate( final Context ctx ) throws ServletException, IOException {
        ctx.contentType( "multipart/form-data" );
        initMultipart( ctx );
        BatchUpdateRequest request;

        String jsonRequest = new BufferedReader( new InputStreamReader( ctx.req().getPart( "request" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
        request = gson.fromJson( jsonRequest, BatchUpdateRequest.class );

        Transaction transaction = getTransaction();
        Statement statement;
        QueryLanguage language = QueryLanguage.from( "sql" );
        List<Result<?, ?>> results = new ArrayList<>();
        for ( Update update : request.updates ) {
            statement = transaction.createStatement();
            String query = update.getQuery( request.tableId, statement, ctx.req() );

            results.add( LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query )
                            .language( language )
                            .origin( ORIGIN )
                            .transactionManager( transactionManager )
                            .build(), UIRequest.builder().build() ).get( 0 ) );
        }
        ctx.json( results );
    }


    /**
     * Get the columns of a table
     */
    void getColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        List<UiColumnDefinition> cols = new ArrayList<>();

        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();
        List<String> primaryColumns;
        if ( table.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
            primaryColumns = new ArrayList<>( primaryKey.getFieldNames() );
        } else {
            primaryColumns = new ArrayList<>();
        }
        for ( LogicalColumn logicalColumn : Catalog.snapshot().rel().getColumns( table.id ) ) {
            String defaultValue = logicalColumn.defaultValue == null ? null : logicalColumn.defaultValue.value.toJson();
            String collectionsType = logicalColumn.collectionsType == null ? "" : logicalColumn.collectionsType.getName();
            cols.add(
                    UiColumnDefinition.builder()
                            .name( logicalColumn.name )
                            .dataType( logicalColumn.type.getName() )
                            .collectionsType( collectionsType )
                            .nullable( logicalColumn.nullable )
                            .precision( logicalColumn.length )
                            .scale( logicalColumn.scale )
                            .dimension( logicalColumn.dimension )
                            .cardinality( logicalColumn.cardinality )
                            .primary( primaryColumns.contains( logicalColumn.name ) )
                            .defaultValue( defaultValue )
                            .build() );
        }
        RelationalResultBuilder<?, ?> result = RelationalResult
                .builder()
                .header( cols.toArray( new UiColumnDefinition[0] ) );
        if ( table.entityType == EntityType.ENTITY ) {
            result.type( ResultType.TABLE );
        } else if ( table.entityType == EntityType.MATERIALIZED_VIEW ) {
            result.type( ResultType.MATERIALIZED );
        } else {
            result.type( ResultType.VIEW );
        }

        ctx.json( result.build() );
    }


    void getDataSourceColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();

        if ( table.entityType == EntityType.VIEW ) {

            List<UiColumnDefinition> columns = new ArrayList<>();
            List<LogicalColumn> cols = Catalog.snapshot().rel().getColumns( table.id );
            for ( LogicalColumn col : cols ) {
                columns.add( UiColumnDefinition.builder()
                        .name( col.name )
                        .dataType( col.type.getName() )
                        .collectionsType( col.collectionsType == null ? "" : col.collectionsType.getName() )
                        .nullable( col.nullable )
                        .precision( col.length )
                        .scale( col.scale )
                        .dimension( col.dimension )
                        .cardinality( col.cardinality )
                        .primary( false )
                        .defaultValue( col.defaultValue == null ? null : col.defaultValue.value.toJson() )
                        .build()
                );

            }
            ctx.json( RelationalResult.builder().header( columns.toArray( new UiColumnDefinition[0] ) ).type( ResultType.VIEW ).build() );
        } else {
            List<AllocationEntity> allocs = Catalog.snapshot().alloc().getFromLogical( table.id );
            if ( Catalog.snapshot().alloc().getFromLogical( table.id ).size() != 1 ) {
                throw new GenericRuntimeException( "The table has an unexpected number of placements!" );
            }

            long adapterId = allocs.get( 0 ).adapterId;
            LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
            List<String> pkColumnNames = primaryKey.getFieldNames();
            List<UiColumnDefinition> columns = new ArrayList<>();
            for ( AllocationColumn ccp : Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( adapterId, table.id ) ) {
                LogicalColumn col = Catalog.snapshot().rel().getColumn( ccp.columnId ).orElseThrow();
                columns.add( UiColumnDefinition.builder()
                        .name( col.name )
                        .dataType( col.type.getName() )
                        .collectionsType( col.collectionsType == null ? "" : col.collectionsType.getName() ).nullable( col.nullable )
                        .precision( col.length )
                        .scale( col.scale )
                        .dimension( col.dimension )
                        .cardinality( col.cardinality )
                        .primary( pkColumnNames.contains( col.name ) )
                        .defaultValue( col.defaultValue == null ? null : col.defaultValue.value.toJson() ).build() );
            }
            ctx.json( RelationalResult.builder().header( columns.toArray( new UiColumnDefinition[0] ) ).type( ResultType.TABLE ).build() );
        }
    }


    /**
     * Get additional columns of the DataSource that are not mapped to the table.
     */
    void getAvailableSourceColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();
        Map<Long, List<Long>> placements = Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id );
        Set<Long> adapterIds = placements.keySet();
        if ( adapterIds.size() > 1 ) {
            LogicalNamespace namespace = Catalog.snapshot().getNamespace( table.namespaceId ).orElseThrow();
            log.warn( String.format( "The number of sources of an entity should not be > 1 (%s.%s)", namespace.name, table.name ) );
        }
        List<RelationalResult> exportedColumns = new ArrayList<>();
        for ( Long adapterId : adapterIds ) {
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( adapterId ).orElseThrow();
            if ( adapter instanceof DataSource<?> dataSource ) {
                for ( Entry<String, List<ExportedColumn>> entry : dataSource.asRelationalDataSource().getExportedColumns().entrySet() ) {
                    List<UiColumnDefinition> columnList = new ArrayList<>();
                    for ( ExportedColumn col : entry.getValue() ) {
                        UiColumnDefinition dbCol = UiColumnDefinition.builder()
                                .name( col.name() )
                                .dataType( col.type().getName() )
                                .collectionsType( col.collectionsType() == null ? "" : col.collectionsType().getName() )
                                .nullable( col.nullable() )
                                .precision( col.length() )
                                .scale( col.scale() )
                                .dimension( col.dimension() )
                                .cardinality( col.cardinality() )
                                .primary( col.primary() )
                                .build();
                        columnList.add( dbCol );
                    }
                    exportedColumns.add( RelationalResult.builder().header( columnList.toArray( new UiColumnDefinition[0] ) ).table( entry.getKey() ).build() );
                    columnList.clear();
                }
                ctx.json( exportedColumns.toArray( new RelationalResult[0] ) );
                return;

            }
        }

        ctx.json( RelationalResult.builder().error( "Could not retrieve exported source fields." ).build() );
    }


    void getMaterializedInfo( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        Pair<LogicalNamespace, LogicalTable> namespaceTable = getNamespaceTable( request );

        LogicalTable table = getLogicalTable( namespaceTable.left.name, namespaceTable.right.name );

        if ( table.entityType == EntityType.MATERIALIZED_VIEW ) {
            LogicalMaterializedView logicalMaterializedView = (LogicalMaterializedView) table;

            MaterializedCriteria materializedCriteria = logicalMaterializedView.getMaterializedCriteria();

            ArrayList<String> materializedInfo = new ArrayList<>();
            materializedInfo.add( materializedCriteria.getCriteriaType().toString() );
            materializedInfo.add( materializedCriteria.getLastUpdate().toString() );
            if ( materializedCriteria.getCriteriaType() == CriteriaType.INTERVAL ) {
                materializedInfo.add( materializedCriteria.getInterval().toString() );
                materializedInfo.add( materializedCriteria.getTimeUnit().name() );
            } else if ( materializedCriteria.getCriteriaType() == CriteriaType.UPDATE ) {
                materializedInfo.add( materializedCriteria.getInterval().toString() );
                materializedInfo.add( "" );
            } else {
                materializedInfo.add( "" );
                materializedInfo.add( "" );
            }

            ctx.json( new MaterializedInfos( materializedInfo ) );
        } else {
            throw new GenericRuntimeException( "only possible with materialized views" );
        }
    }


    private LogicalTable getLogicalTable( String namespace, String table ) {
        return Catalog.snapshot().rel().getTable( namespace, table ).orElseThrow();
    }


    void updateMaterialized( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        List<String> queries = new ArrayList<>();
        StringBuilder sBuilder = new StringBuilder();

        String tableId = getFullEntityName( request.entityId );

        String query = String.format( "ALTER MATERIALIZED VIEW %s FRESHNESS MANUAL", tableId );
        queries.add( query );

        for ( String q : queries ) {
            sBuilder.append( q );

            QueryLanguage language = QueryLanguage.from( "sql" );
            Result<?, ?> result = LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query )
                            .language( language )
                            .origin( ORIGIN )
                            .transactionManager( transactionManager )
                            .build(), UIRequest.builder().build() ).get( 0 );
            ctx.json( result );

        }
    }


    void updateColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );
        if ( !ensureTableModifiable( request.entityId, ctx ) ) {
            return;
        }

        UiColumnDefinition oldColumn = request.oldColumn;
        UiColumnDefinition newColumn = request.newColumn;
        List<String> queries = new ArrayList<>();
        StringBuilder sBuilder = new StringBuilder();

        String tableId = getFullEntityName( request.entityId );

        // rename column if needed
        if ( !oldColumn.name.equals( newColumn.name ) ) {
            String query;
            if ( request.tableType.equals( "VIEW" ) ) {
                query = String.format( "ALTER VIEW %s RENAME COLUMN \"%s\" TO \"%s\"", tableId, oldColumn.name, newColumn.name );
            } else if ( request.tableType.equals( "MATERIALIZED" ) ) {
                query = String.format( "ALTER MATERIALIZED VIEW %s RENAME COLUMN \"%s\" TO \"%s\"", tableId, oldColumn.name, newColumn.name );
            } else {
                query = String.format( "ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\"", tableId, oldColumn.name, newColumn.name );
            }
            queries.add( query );
        }

        if ( !request.renameOnly ) {
            // change type + length
            // TODO: cast if needed
            if ( !oldColumn.dataType.equals( newColumn.dataType ) ||
                    !Objects.equals( oldColumn.collectionsType, newColumn.collectionsType ) ||
                    !Objects.equals( oldColumn.precision, newColumn.precision ) ||
                    !Objects.equals( oldColumn.scale, newColumn.scale ) ||
                    !oldColumn.dimension.equals( newColumn.dimension ) ||
                    !oldColumn.cardinality.equals( newColumn.cardinality ) ) {
                // TODO: drop maxlength if requested
                String query = String.format( "ALTER TABLE %s MODIFY COLUMN \"%s\" SET TYPE %s", tableId, newColumn.name, newColumn.dataType );
                if ( newColumn.precision != null ) {
                    query = query + "(" + newColumn.precision;
                    if ( newColumn.scale != null ) {
                        query = query + "," + newColumn.scale;
                    }
                    query = query + ")";
                }
                //collectionType
                if ( newColumn.collectionsType != null && !newColumn.collectionsType.isEmpty() ) {
                    query = query + " " + request.newColumn.collectionsType;
                    int dimension = newColumn.dimension == null ? -1 : newColumn.dimension;
                    int cardinality = newColumn.cardinality == null ? -1 : newColumn.cardinality;
                    query = query + String.format( "(%d,%d)", dimension, cardinality );
                }
                queries.add( query );
            }

            // set/drop nullable
            if ( oldColumn.nullable != newColumn.nullable ) {
                String nullable = "SET";
                if ( newColumn.nullable ) {
                    nullable = "DROP";
                }
                String query = "ALTER TABLE " + tableId + " MODIFY COLUMN \"" + newColumn.name + "\" " + nullable + " NOT NULL";
                queries.add( query );
            }

            // change default value
            if ( !Objects.equals( oldColumn.defaultValue, newColumn.defaultValue ) ) {
                String query;
                if ( newColumn.defaultValue == null ) {
                    query = String.format( "ALTER TABLE %s MODIFY COLUMN \"%s\" DROP DEFAULT", tableId, newColumn.name );
                } else {
                    query = String.format( "ALTER TABLE %s MODIFY COLUMN \"%s\" SET DEFAULT ", tableId, newColumn.name );
                    if ( newColumn.collectionsType != null ) {
                        //handle the case if the user says "ARRAY[1,2,3]" or "[1,2,3]"
                        if ( !request.newColumn.defaultValue.startsWith( request.newColumn.collectionsType ) ) {
                            query = query + request.newColumn.collectionsType;
                        }
                        query = query + request.newColumn.defaultValue;
                    } else {
                        switch ( newColumn.dataType ) {
                            case "BIGINT":
                            case "INTEGER":
                            case "DECIMAL":
                            case "DOUBLE":
                            case "FLOAT":
                            case "SMALLINT":
                            case "TINYINT":
                                String defaultValue = request.newColumn.defaultValue.replace( ",", "." );
                                BigDecimal b = new BigDecimal( defaultValue );
                                query = query + b;
                                break;
                            case "VARCHAR":
                                query = query + String.format( "'%s'", request.newColumn.defaultValue );
                                break;
                            default:
                                query = query + request.newColumn.defaultValue;
                        }
                    }
                }
                queries.add( query );
            }
        }

        for ( String query : queries ) {
            sBuilder.append( query );
            QueryLanguage language = QueryLanguage.from( "sql" );
            Result<?, ?> result = LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query )
                            .language( language )
                            .origin( ORIGIN )
                            .transactionManager( transactionManager )
                            .build(), UIRequest.builder().build() ).get( 0 );
            ctx.json( result );
            if ( result.error != null ) {
                break;
            }
        }
    }


    /**
     * Add a column to an existing table
     */
    void addColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );
        if ( !ensureTableModifiable( request.entityId, ctx ) ) {
            return;
        }

        String tableId = getFullEntityName( request.entityId );

        String as = "";
        String dataType = request.newColumn.dataType;
        if ( request.newColumn.as != null ) {
            //for data sources
            as = "AS \"" + request.newColumn.as + "\"";
            dataType = "";
        }
        String query = String.format( "ALTER TABLE %s ADD COLUMN \"%s\" %s %s", tableId, request.newColumn.name, as, dataType );
        //we don't want precision, scale etc. for source columns
        if ( request.newColumn.as == null ) {
            if ( request.newColumn.precision != null ) {
                query = query + "(" + request.newColumn.precision;
                if ( request.newColumn.scale != null ) {
                    query = query + "," + request.newColumn.scale;
                }
                query = query + ")";
            }
            if ( request.newColumn.collectionsType != null && !request.newColumn.collectionsType.isEmpty() ) {
                query = query + " " + request.newColumn.collectionsType;
                int dimension = request.newColumn.dimension == null ? -1 : request.newColumn.dimension;
                int cardinality = request.newColumn.cardinality == null ? -1 : request.newColumn.cardinality;
                query = query + String.format( "(%d,%d)", dimension, cardinality );
            }
            if ( !request.newColumn.nullable ) {
                query = query + " NOT NULL";
            }
        }
        if ( request.newColumn.defaultValue != null && !request.newColumn.defaultValue.isEmpty() ) {
            query = query + " DEFAULT ";
            if ( request.newColumn.collectionsType != null && !request.newColumn.collectionsType.isEmpty() ) {
                //handle the case if the user says "ARRAY[1,2,3]" or "[1,2,3]"
                if ( !request.newColumn.defaultValue.startsWith( request.newColumn.collectionsType ) ) {
                    query = query + request.newColumn.collectionsType;
                }
                query = query + request.newColumn.defaultValue;
            } else {
                switch ( request.newColumn.dataType ) {
                    case "BIGINT":
                    case "INTEGER":
                    case "SMALLINT":
                    case "TINYINT":
                    case "FLOAT":
                    case "DOUBLE":
                    case "DECIMAL":
                        String defaultValue = request.newColumn.defaultValue.replace( ",", "." );
                        BigDecimal b = new BigDecimal( defaultValue );
                        query = query + b;
                        break;
                    case "VARCHAR":
                        query = query + String.format( "'%s'", request.newColumn.defaultValue );
                        break;
                    default:
                        query = query + request.newColumn.defaultValue;
                }
            }
        }
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    /**
     * Delete a column of a table
     */
    void dropColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );
        if ( !ensureTableModifiable( request.entityId, ctx ) ) {
            return;
        }

        String tableId = getFullEntityName( request.entityId );
        String query = String.format( "ALTER TABLE %s DROP COLUMN \"%s\"", tableId, request.oldColumn.name );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    /**
     * Get artificially generated index/foreign key/constraint names for placeholders in the UI
     */
    void getGeneratedNames( final Context ctx ) {
        String[] data = new String[3];
        data[0] = NameGenerator.generateConstraintName();
        data[1] = NameGenerator.generateForeignKeyName();
        data[2] = NameGenerator.generateIndexName();
        ctx.json( RelationalResult.builder().header( new UiColumnDefinition[0] ).data( new String[][]{ data } ).build() );
    }


    /**
     * Get constraints of a table
     */
    void getConstraints( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        RelationalResult result;

        List<TableConstraint> resultList = new ArrayList<>();
        Map<String, List<String>> temp = new HashMap<>();

        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();

        // get primary key
        if ( table.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
            for ( String columnName : primaryKey.getFieldNames() ) {
                if ( !temp.containsKey( "" ) ) {
                    temp.put( "", new ArrayList<>() );
                }
                temp.get( "" ).add( columnName );
            }
            for ( Map.Entry<String, List<String>> entry : temp.entrySet() ) {
                resultList.add( new TableConstraint( entry.getKey(), "PRIMARY KEY", entry.getValue() ) );
            }
        }

        // get unique constraints.
        temp.clear();
        List<LogicalConstraint> constraints = Catalog.snapshot().rel().getConstraints( table.id );
        for ( LogicalConstraint logicalConstraint : constraints ) {
            if ( logicalConstraint.type == ConstraintType.UNIQUE ) {
                temp.put( logicalConstraint.name, new ArrayList<>( logicalConstraint.key.getFieldNames() ) );
            }
        }
        for ( Map.Entry<String, List<String>> entry : temp.entrySet() ) {
            resultList.add( new TableConstraint( entry.getKey(), "UNIQUE", entry.getValue() ) );
        }

        // the foreign keys are listed separately

        UiColumnDefinition[] header = { UiColumnDefinition.builder().name( "Name" ).build(), UiColumnDefinition.builder().name( "Type" ).build(), UiColumnDefinition.builder().name( "Columns" ).build() };
        List<String[]> data = new ArrayList<>();
        resultList.forEach( c -> data.add( c.asRow() ) );

        result = RelationalResult.builder().header( header ).data( data.toArray( new String[0][2] ) ).build();

        ctx.json( result );
    }


    void dropConstraint( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );
        if ( !ensureTableModifiable( request.entityId, ctx ) ) {
            return;
        }

        long entityId = request.entityId;
        String fullEntityName = getFullEntityName( entityId );

        String query = getDropConstraintQuery( request, fullEntityName );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    private static String getDropConstraintQuery( ConstraintRequest request, String fullEntityName ) {
        String query;
        if ( request.constraint.type.equals( ConstraintType.PRIMARY.name() ) ) {
            query = String.format( "ALTER TABLE %s DROP PRIMARY KEY", fullEntityName );
        } else if ( request.constraint.type.equals( ConstraintType.FOREIGN.name() ) ) {
            query = String.format( "ALTER TABLE %s DROP FOREIGN KEY \"%s\"", fullEntityName, request.constraint.name );
        } else {
            query = String.format( "ALTER TABLE %s DROP CONSTRAINT \"%s\"", fullEntityName, request.constraint.name );
        }
        return query;
    }


    /**
     * Add a primary key to a table
     */
    void addPrimaryKey( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );
        if ( !ensureTableModifiable( request.entityId, ctx ) ) {
            return;
        }

        long entityId = request.entityId;
        String tableId = getFullEntityName( entityId );

        RelationalResult result;
        if ( request.constraint.columns.length < 1 ) {
            result = RelationalResult.builder().error( "Cannot add primary key if no columns are provided." ).build();
            ctx.json( result );
            return;
        }
        StringJoiner joiner = new StringJoiner( ",", "(", ")" );
        for ( String s : request.constraint.columns ) {
            joiner.add( "\"" + s + "\"" );
        }
        String query = "ALTER TABLE " + tableId + " ADD PRIMARY KEY " + joiner;
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );

        ctx.json( res );
    }


    /**
     * Add a primary key to a table
     */
    void addUniqueConstraint( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );
        if ( !ensureTableModifiable( request.entityId, ctx ) ) {
            return;
        }

        long entityId = request.entityId;
        String tableName = getFullEntityName( entityId );

        Result<?, ?> result;
        if ( request.constraint.columns.length > 0 ) {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( String s : request.constraint.columns ) {
                joiner.add( "\"" + s + "\"" );
            }
            String query = "ALTER TABLE " + tableName + " ADD CONSTRAINT \"" + request.constraint.name + "\" UNIQUE " + joiner;
            QueryLanguage language = QueryLanguage.from( "sql" );
            result = LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query )
                            .language( language )
                            .origin( ORIGIN )
                            .transactionManager( transactionManager )
                            .build(), UIRequest.builder().build() ).get( 0 );
        } else {
            result = RelationalResult.builder().error( "Cannot add unique constraint if no columns are provided." ).build();
        }
        ctx.json( result );
    }


    /**
     * Get indexes of a table
     */
    void getIndexes( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        Pair<LogicalNamespace, LogicalTable> namespaceTable = getNamespaceTable( request );

        LogicalTable table = getLogicalTable( namespaceTable.left.name, namespaceTable.right.name );
        List<LogicalIndex> logicalIndices = Catalog.snapshot().rel().getIndexes( table.id, false );

        UiColumnDefinition[] header = {
                UiColumnDefinition.builder().name( "Name" ).build(),
                UiColumnDefinition.builder().name( "Columns" ).build(),
                UiColumnDefinition.builder().name( "Location" ).build(),
                UiColumnDefinition.builder().name( "Method" ).build(),
                UiColumnDefinition.builder().name( "Type" ).build() };

        List<String[]> data = new ArrayList<>();

        // Get explicit indexes
        for ( LogicalIndex logicalIndex : logicalIndices ) {
            String[] arr = new String[5];
            String storeUniqueName;
            if ( logicalIndex.location < 0 ) {
                // a polystore index
                storeUniqueName = "Polypheny-DB";
            } else {
                storeUniqueName = Catalog.snapshot().getAdapter( logicalIndex.location ).orElseThrow().uniqueName;
            }
            arr[0] = logicalIndex.name;
            arr[1] = String.join( ", ", logicalIndex.key.getFieldNames() );
            arr[2] = storeUniqueName;
            arr[3] = logicalIndex.methodDisplayName;
            arr[4] = logicalIndex.type.name();
            data.add( arr );
        }

        // Get functional indexes
        List<AllocationEntity> allocs = Catalog.snapshot().alloc().getFromLogical( table.id );
        for ( AllocationEntity alloc : allocs ) {
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( alloc.adapterId ).orElseThrow();
            DataStore<?> store;
            if ( adapter instanceof DataStore<?> ) {
                store = (DataStore<?>) adapter;
            } else {
                break;
            }
            for ( FunctionalIndexInfo fif : store.getFunctionalIndexes( table ) ) {
                String[] arr = new String[5];
                arr[0] = "";
                arr[1] = String.join( ", ", fif.getColumnNames() );
                arr[2] = store.getUniqueName();
                arr[3] = fif.methodDisplayName();
                arr[4] = "FUNCTIONAL";
                data.add( arr );
            }
        }

        ctx.json( RelationalResult.builder().header( header ).data( data.toArray( new String[0][2] ) ).build() );
    }


    /**
     * Drop an index of a table
     */
    void dropIndex( final Context ctx ) {
        IndexModel index = ctx.bodyAsClass( IndexModel.class );
        if ( !ensureTableModifiable( index.entityId, ctx ) ) {
            return;
        }

        String tableName = getFullEntityName( index.entityId );
        String query = String.format( "ALTER TABLE %s DROP INDEX \"%s\"", tableName, index.getName() );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    /**
     * Create an index for a table
     */
    void createIndex( final Context ctx ) {
        IndexModel index = ctx.bodyAsClass( IndexModel.class );
        if ( !ensureTableModifiable( index.entityId, ctx ) ) {
            return;
        }

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( index.namespaceId ).orElseThrow();
        LogicalTable table = Catalog.snapshot().rel().getTable( index.entityId ).orElseThrow();

        String tableId = String.format( "\"%s\".\"%s\"", namespace.name, table.name );
        StringJoiner colJoiner = new StringJoiner( ",", "(", ")" );
        for ( long col : index.columnIds ) {
            colJoiner.add( "\"" + Catalog.snapshot().rel().getColumn( col ).orElseThrow().name + "\"" );
        }
        String store = IndexManager.POLYPHENY;
        if ( index.storeUniqueName != null && !index.storeUniqueName.equals( "Polypheny-DB" ) ) {
            store = index.getStoreUniqueName();
        }
        String onStore = String.format( "ON STORE \"%s\"", store );

        String query = String.format( "ALTER TABLE %s ADD INDEX \"%s\" ON %s USING \"%s\" %s", tableId, index.getName(), colJoiner, index.getMethod(), onStore );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    void getUnderlyingTable( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        LogicalTable table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();

        if ( table.entityType == EntityType.VIEW ) {
            ImmutableMap<Long, List<Long>> underlyingTableOriginal = table.unwrapOrThrow( LogicalView.class ).underlyingTables;
            Map<String, List<String>> underlyingTable = new HashMap<>();
            for ( Entry<Long, List<Long>> entry : underlyingTableOriginal.entrySet() ) {
                List<String> columns = new ArrayList<>();
                for ( Long ids : entry.getValue() ) {
                    columns.add( Catalog.snapshot().rel().getColumn( ids ).orElseThrow().name );
                }
                underlyingTable.put( Catalog.snapshot().rel().getTable( entry.getKey() ).orElseThrow().name, columns );
            }
            ctx.json( new UnderlyingTables( underlyingTable ) );
        } else {
            throw new GenericRuntimeException( "Only possible with Views" );
        }
    }


    /**
     * Get placements of a table
     */
    void getPlacements( final Context ctx ) {
        IndexModel index = ctx.bodyAsClass( IndexModel.class );
        ctx.json( getPlacements( index ) );
    }


    private PlacementModel getPlacements( final IndexModel index ) {
        Snapshot snapshot = Catalog.snapshot();

        LogicalTable table = Catalog.snapshot().rel().getTable( index.entityId ).orElseThrow();
        PlacementModel p = new PlacementModel( snapshot.alloc().getFromLogical( table.id ).size() > 1, snapshot.alloc().getPartitionGroupNames( table.id ), table.entityType );
        if ( table.entityType != EntityType.VIEW ) {
            long pkid = table.primaryKey;
            List<Long> pkColumnIds = snapshot.rel().getPrimaryKey( pkid ).orElseThrow().fieldIds;
            LogicalColumn pkColumn = snapshot.rel().getColumn( pkColumnIds.get( 0 ) ).orElseThrow();
            List<AllocationColumn> pkPlacements = snapshot.alloc().getColumnFromLogical( pkColumn.id ).orElseThrow();
            for ( AllocationColumn placement : pkPlacements ) {
                Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId ).orElseThrow();
                PartitionProperty property = snapshot.alloc().getPartitionProperty( table.id ).orElseThrow();
                p.addAdapter( new RelationalStore(
                        adapter.getUniqueName(),
                        adapter.getUniqueName(),
                        snapshot.alloc().getColumnPlacementsOnAdapterPerEntity( adapter.getAdapterId(), table.id ),
                        snapshot.alloc().getPartitionGroupsIndexOnDataPlacement( placement.adapterId, placement.logicalTableId ),
                        property.numPartitionGroups,
                        property.partitionType ) );
            }
        }
        return p;
    }


    /**
     * Add or drop a data placement.
     * Parameter of type models.Index: index name corresponds to storeUniqueName
     * Index method: either 'ADD' or 'DROP'
     */
    void addDropPlacement( final Context ctx ) {
        PlacementFieldsModel placementFields = ctx.bodyAsClass( PlacementFieldsModel.class );
        if ( !ensureTableModifiable( placementFields.entityId(), ctx ) ) {
            return;
        }
        if ( placementFields.method() == null ) {
            ctx.json( RelationalResult.builder().error( "Invalid request" ).build() );
            return;
        }
        StringJoiner columnJoiner = new StringJoiner( ",", "(", ")" );
        int counter = 0;
        if ( placementFields.method() != PlacementFieldsModel.Method.DROP ) {
            for ( String name : placementFields.fieldNames() ) {
                columnJoiner.add( "\"" + name + "\"" );
                counter++;
            }
        }
        String columnListStr = counter > 0 ? columnJoiner.toString() : "";
        String query = String.format(
                "ALTER TABLE \"%s\".\"%s\" %s PLACEMENT %s ON STORE \"%s\"",
                Catalog.snapshot().getNamespace( placementFields.namespaceId() ).orElseThrow().name,
                Catalog.snapshot().rel().getTable( placementFields.entityId() ).orElseThrow().name,
                placementFields.method().name(),
                columnListStr,
                placementFields.adapterName() );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    void getPartitionTypes( final Context ctx ) {
        ctx.json( Arrays.stream( PartitionType.values() ).filter( t -> t != PartitionType.NONE ).toArray( PartitionType[]::new ) );
    }


    private List<PartitionFunctionColumn> buildPartitionFunctionRow( PartitioningRequest request, List<PartitionFunctionInfoColumn> columnList ) {
        List<PartitionFunctionColumn> constructedRow = new ArrayList<>();

        for ( PartitionFunctionInfoColumn currentColumn : columnList ) {
            FieldType type = switch ( currentColumn.getFieldType() ) {
                case STRING -> FieldType.STRING;
                case INTEGER -> FieldType.INTEGER;
                case LIST -> FieldType.LIST;
                case LABEL -> FieldType.LABEL;
            };

            if ( type.equals( FieldType.LIST ) ) {
                constructedRow.add( new PartitionFunctionColumn( type, currentColumn.getOptions(), currentColumn.getDefaultValue() )
                        .setModifiable( currentColumn.isModifiable() )
                        .setMandatory( currentColumn.isMandatory() )
                        .setSqlPrefix( currentColumn.getSqlPrefix() )
                        .setSqlSuffix( currentColumn.getSqlSuffix() ) );
            } else {

                String defaultValue = getDefaultValue( request, currentColumn, type );

                constructedRow.add( new PartitionFunctionColumn( type, defaultValue )
                        .setModifiable( currentColumn.isModifiable() )
                        .setMandatory( currentColumn.isMandatory() )
                        .setSqlPrefix( currentColumn.getSqlPrefix() )
                        .setSqlSuffix( currentColumn.getSqlSuffix() ) );
            }
        }

        return constructedRow;
    }


    private static String getDefaultValue( PartitioningRequest request, PartitionFunctionInfoColumn currentColumn, FieldType type ) {
        String defaultValue = currentColumn.getDefaultValue();

        // Used specifically for Temp-Partitioning since number of selected partitions remains 2 but chunks change
        // enables user to use selected "number of partitions" being used as default value for "number of internal data chunks"
        if ( request.method.equals( PartitionType.TEMPERATURE ) ) {

            if ( type.equals( FieldType.STRING ) && currentColumn.getDefaultValue().equals( "-04071993" ) ) {
                defaultValue = String.valueOf( request.numPartitions );
            }
        }
        return defaultValue;
    }


    void getPartitionFunctionModel( final Context ctx ) {
        PartitioningRequest request = ctx.bodyAsClass( PartitioningRequest.class );

        // Get correct partition function
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( request.method );

        // Check whether the selected partition function supports the selected partition column
        LogicalColumn partitionColumn;

        LogicalNamespace namespace = Catalog.snapshot().getNamespace( request.schemaName ).orElseThrow();

        partitionColumn = Catalog.snapshot().rel().getColumn( namespace.id, request.tableName, request.column ).orElseThrow();

        if ( !partitionManager.supportsColumnOfType( partitionColumn.type ) ) {
            ctx.json( new PartitionFunctionModel( "The partition function " + request.method + " does not support columns of type " + partitionColumn.type ) );
            return;
        }

        PartitionFunctionInfo functionInfo = partitionManager.getPartitionFunctionInfo();

        JsonObject infoJson = gson.toJsonTree( partitionManager.getPartitionFunctionInfo() ).getAsJsonObject();

        List<List<PartitionFunctionColumn>> rows = new ArrayList<>();

        if ( infoJson.has( "rowsBefore" ) ) {
            // Insert Rows Before
            List<List<PartitionFunctionInfoColumn>> rowsBefore = functionInfo.getRowsBefore();
            for ( List<PartitionFunctionInfoColumn> partitionFunctionInfoColumns : rowsBefore ) {
                rows.add( buildPartitionFunctionRow( request, partitionFunctionInfoColumns ) );
            }
        }

        if ( infoJson.has( "dynamicRows" ) ) {
            // Build as many dynamic rows as requested per num Partitions
            for ( int i = 0; i < request.numPartitions; i++ ) {
                rows.add( buildPartitionFunctionRow( request, functionInfo.getDynamicRows() ) );
            }
        }

        if ( infoJson.has( "rowsAfter" ) ) {
            // Insert Rows After
            List<List<PartitionFunctionInfoColumn>> rowsAfter = functionInfo.getRowsAfter();
            for ( List<PartitionFunctionInfoColumn> partitionFunctionInfoColumns : rowsAfter ) {
                rows.add( buildPartitionFunctionRow( request, partitionFunctionInfoColumns ) );
            }
        }

        PartitionFunctionModel model = new PartitionFunctionModel( functionInfo.getFunctionTitle(), functionInfo.getDescription(), functionInfo.getHeadings(), rows );
        model.setFunctionName( request.method.toString() );
        model.setTableName( request.tableName );
        model.setPartitionColumnName( request.column );
        model.setSchemaName( request.schemaName );

        ctx.json( model );
    }


    void partitionTable( final Context ctx ) {
        PartitionFunctionModel request = ctx.bodyAsClass( PartitionFunctionModel.class );
        if ( !ensureTableModifiable( request.schemaName, request.tableName, ctx ) ) {
            return;
        }

        // Get correct partition function
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( PartitionType.getByName( request.functionName ) );

        PartitionFunctionInfo functionInfo = partitionManager.getPartitionFunctionInfo();

        StringBuilder content = new StringBuilder();
        for ( List<PartitionFunctionColumn> currentRow : request.rows ) {
            boolean rowSeparationApplied = false;
            for ( PartitionFunctionColumn currentColumn : currentRow ) {
                if ( currentColumn.modifiable ) {
                    // If more than one row, keep appending ','
                    if ( !rowSeparationApplied && request.rows.indexOf( currentRow ) != 0 ) {
                        content.append( functionInfo.getRowSeparation() );
                        rowSeparationApplied = true;
                    }
                    content.append( currentColumn.sqlPrefix ).append( " " ).append( currentColumn.value ).append( " " ).append( currentColumn.sqlSuffix );
                }
            }
        }

        content = new StringBuilder( functionInfo.getSqlPrefix() + " " + content + " " + functionInfo.getSqlSuffix() );

        //INFO - do discuss
        //Problem is that we took the structure completely out of the original JSON therefore losing valuable information and context
        //what part of rows were actually needed to build the SQL and which one not.
        //Now we have to crosscheck every statement
        //Actually to complex and rather poor maintenance quality.
        //Changes to extensions to this model now have to be made on two parts

        String query = String.format( "ALTER TABLE \"%s\".\"%s\" PARTITION BY %s (\"%s\") %s ",
                request.schemaName, request.tableName, request.functionName, request.partitionColumnName, content );

        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    void mergePartitions( final Context ctx ) {
        PartitioningRequest request = ctx.bodyAsClass( PartitioningRequest.class );
        if ( !ensureTableModifiable( request.schemaName, request.tableName, ctx ) ) {
            return;
        }
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" MERGE PARTITIONS", request.schemaName, request.tableName );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    void modifyPartitions( final Context ctx ) {
        ModifyPartitionRequest request = ctx.bodyAsClass( ModifyPartitionRequest.class );
        if ( !ensureTableModifiable( request.schemaName, request.tableName, ctx ) ) {
            return;
        }
        StringJoiner partitions = new StringJoiner( "," );
        for ( String partition : request.partitions ) {
            partitions.add( "\"" + partition + "\"" );
        }
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" MODIFY PARTITIONS(%s) ON STORE %s", request.schemaName, request.tableName, partitions, request.storeUniqueName );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    /**
     * Get deployed data stores
     */
    void getStores( final Context ctx ) {
        ImmutableMap<String, DataStore<?>> stores = AdapterManager.getInstance().getStores();
        DataStore<?>[] out = stores.values().toArray( new DataStore[0] );
        ctx.json( out );
    }


    /**
     * Get the available stores on which a new index can be placed. 'Polypheny-DB' is part of the list, if polystore-indexes are enabled
     */
    void getAvailableStoresForIndexes( final Context ctx ) {
        IndexModel index = ctx.bodyAsClass( IndexModel.class );
        PlacementModel dataPlacements = getPlacements( index );
        Map<String, DataStore<?>> stores = AdapterManager.getInstance().getStores();
        List<IndexAdapterModel> filtered = stores.values().stream().filter( ( s ) -> {
            if ( s.getAvailableIndexMethods() == null || s.getAvailableIndexMethods().isEmpty() ) {
                return false;
            }
            return dataPlacements.stores.stream().anyMatch( ( dp ) -> dp.uniqueName.equals( s.getUniqueName() ) );
        } ).map( IndexAdapterModel::from ).collect( Collectors.toCollection( ArrayList::new ) );

        if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ) {
            IndexAdapterModel poly = new IndexAdapterModel(
                    -1L,
                    "Polypheny-DB",
                    IndexManager.getAvailableIndexMethods().stream().map( IndexMethodModel::from ).toList() );
            filtered.add( poly );
        }
        ctx.json( filtered );
    }


    /**
     * Update the settings of an adapter
     */
    void updateAdapterSettings( final Context ctx ) {
        UpdateAdapterRequest request = ctx.bodyAsClass( UpdateAdapterRequest.class );
        try {
            AdapterManager.getInstance().getAdapter( request.getUniqueName() ).orElseThrow().updateSettings( request.getSettings() );
            Catalog.getInstance().commit();
        } catch ( Throwable t ) {
            ctx.json( RelationalResult.builder().error( "Could not update AdapterSettings: " + t.getMessage() ).build() );
            return;
        }

        // Reset caches (not a nice solution to create a transaction, statement and query processor for doing this, but it
        // currently seems to be the best option). When migrating this to a DDL manager, make sure to find a better approach.
        Transaction transaction = null;
        try {
            transaction = getTransaction();
            transaction.createStatement().getQueryProcessor().resetCaches();
            transaction.commit();
        } catch ( TransactionException e ) {
            String error = "Error while resetting caches: " + e.getMessage();
            if ( transaction != null ) {
                transaction.rollback( error );
            }

            ctx.json( RelationalResult.builder().error( error ).build() );
            return;
        }

        ctx.json( RelationalResult.builder().affectedTuples( 1 ).build() );
    }


    /**
     * Get available adapters
     */
    private void getAvailableAdapters( Context ctx, AdapterType adapterType ) {
        List<AdapterInformation> adapters = AdapterManager.getInstance().getAdapterTemplates( adapterType );
        ctx.json( adapters.toArray( new AdapterInformation[0] ) );
    }


    void getAvailableStores( final Context ctx ) {
        getAvailableAdapters( ctx, AdapterType.STORE );
    }


    void getAvailableSources( final Context ctx ) {
        getAvailableAdapters( ctx, AdapterType.SOURCE );
    }


    /**
     * Get deployed data sources
     */
    void getSources( final Context ctx ) {
        ImmutableMap<String, DataSource<?>> sources = AdapterManager.getInstance().getSources();
        ctx.json( sources.values().toArray( new DataSource<?>[0] ) );
    }


    /**
     * Deploy a new adapter
     */
    void createAdapter( final Context ctx ) throws ServletException, IOException {
        initMultipart( ctx );
        String body = "";
        Map<String, InputStream> inputStreams = new HashMap<>();

        final AdapterModel adapterModel;
        if ( ctx.isMultipartFormData() ) {
            // collect all files e.g. csv files
            for ( Part part : ctx.req().getParts() ) {
                if ( part.getName().equals( "body" ) ) {
                    body = IOUtils.toString( ctx.req().getPart( "body" ).getInputStream(), StandardCharsets.UTF_8 );
                } else {
                    inputStreams.put( part.getName(), part.getInputStream() );
                }
            }
            adapterModel = HttpServer.mapper.readValue( body, AdapterModel.class );
        } else if ( "application/json".equals( ctx.contentType() ) ) {
            adapterModel = ctx.bodyAsClass( AdapterModel.class );
        } else {
            ctx.status( HttpStatus.BAD_REQUEST );
            return;
        }

        if ( adapterModel.name == null || adapterModel.name.isEmpty() ) {
            ctx.status( HttpStatus.BAD_REQUEST );
            ctx.result( "Missing adapter attribute: name" );
            return;
        }

        AdapterTemplate adapterTemplate = AdapterManager.getAdapterTemplate( adapterModel.adapterName, adapterModel.type );

        // This is only used to be able to get the type of property based on the key
        Map<String, AbstractAdapterSetting> defaultSettings = adapterTemplate.settings.stream().collect( Collectors.toMap( e -> e.name, e -> e ) );
        ConnectionMethod method = ConnectionMethod.UPLOAD;
        if ( adapterModel.settings.containsKey( "method" ) ) {
            method = ConnectionMethod.valueOf( adapterModel.settings.get( "method" ).toUpperCase() );
        }

        Map<String, String> adapterSettings = new HashMap<>();

        for ( Map.Entry<String, String> entry : adapterModel.settings.entrySet() ) {
            if ( !defaultSettings.containsKey( entry.getKey() ) ) {
                // specified property is not available for this adapter
                continue;
            }
            adapterSettings.put( entry.getKey(), entry.getValue() );
            AbstractAdapterSetting set = defaultSettings.get( entry.getKey() );
            // handle upload
            if ( (set instanceof AbstractAdapterSettingDirectory settingDirectory) && method == ConnectionMethod.UPLOAD ) {
                List<String> fileNames = HttpServer.mapper.readValue( entry.getValue(), new TypeReference<>() {
                } );
                String directory = handleUploadFiles( inputStreams, fileNames, settingDirectory, adapterModel );
                adapterSettings.put( entry.getKey(), directory );
                continue;
            }
            // handle linking
            if ( (set instanceof AbstractAdapterSettingString settingString) && method == ConnectionMethod.LINK ) {
                if ( !settingString.name.equals( "directoryName" ) ) {
                    continue;
                }
                Exception e = handleLinkFiles( settingString );
                if ( e != null ) {
                    ctx.json( RelationalResult.builder().exception( e ).build() );
                    return;
                }
            }
        }

        adapterSettings.put( "mode", adapterModel.mode.toString() );

        String query = String.format( "ALTER ADAPTERS ADD \"%s\" USING '%s' AS '%s' WITH '%s'", adapterModel.name, adapterModel.adapterName, adapterModel.type, Crud.gson.toJson( adapterSettings ) );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    public void startAccessRequest( Context ctx ) {
        PathAccessRequest request = ctx.bodyAsClass( PathAccessRequest.class );
        UUID uuid = SecurityManager.getInstance().requestPathAccess( request.getName(), ctx.req().getSession().getId(), Path.of( request.getDirectoryName() ) );
        if ( uuid != null ) {
            ctx.json( uuid );
        } else {
            ctx.result( "" );
        }
    }


    private Exception handleLinkFiles( AbstractAdapterSettingString setting ) {
        Path path = Path.of( setting.getValue() );
        SecurityManager.getInstance().requestPathAccess( "webui", "webui", path );
        if ( !SecurityManager.getInstance().checkPathAccess( path ) ) {
            return new GenericRuntimeException( "Security check for access was not successful; not enough permissions." );
        }
        return null;
    }


    private static String handleUploadFiles( Map<String, InputStream> inputStreams, List<String> fileNames, AbstractAdapterSettingDirectory setting, AdapterModel a ) {
        if ( fileNames.isEmpty() ) {
            throw new GenericRuntimeException( "No file or directory specified for upload!" );
        }
        for ( String fileName : fileNames ) {
            setting.inputStreams.put( fileName, inputStreams.get( fileName ) );
        }
        File path = PolyphenyHomeDirManager.getInstance().registerNewFolder( "data/csv/" + a.name );
        for ( Entry<String, InputStream> is : setting.inputStreams.entrySet() ) {
            try {
                File file = new File( path, is.getKey() );
                FileUtils.copyInputStreamToFile( is.getValue(), file );
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
        }
        return path.getAbsolutePath();
    }


    /**
     * Remove an existing storeId or source
     */
    void removeAdapter( final Context ctx ) {
        String uniqueName = ctx.body();
        String query = String.format( "ALTER ADAPTERS DROP \"%s\"", uniqueName );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    void getQueryInterfaces( final Context ctx ) {
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        ImmutableMap<String, QueryInterface> queryInterfaces = qim.getQueryInterfaces();
        List<QueryInterfaceModel> qIs = new ArrayList<>();
        for ( QueryInterface i : queryInterfaces.values() ) {
            qIs.add( new QueryInterfaceModel( i ) );
        }
        ctx.json( qIs.toArray( new QueryInterfaceModel[0] ) );
    }


    void getAvailableQueryInterfaces( final Context ctx ) {
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        ctx.json( qim.getAvailableQueryInterfaceTemplates() );
    }


    void createQueryInterface( final Context ctx ) {
        QueryInterfaceCreateRequest request = ctx.bodyAsClass( QueryInterfaceCreateRequest.class );
        try {
            QueryInterfaceManager.getInstance().createQueryInterface( request.interfaceType(), request.uniqueName(), request.settings() );
            ctx.status( 200 );
        } catch ( RuntimeException e ) {
            log.error( "Exception while deploying query interface", e );
            ctx.status( 500 ).result( e.getMessage() );
        }
    }


    void updateQueryInterfaceSettings( final Context ctx ) {
        QueryInterfaceModel request = ctx.bodyAsClass( QueryInterfaceModel.class );
        try {
            QueryInterfaceManager.getInstance().getQueryInterface( request.uniqueName ).updateSettings( request.currentSettings );
            ctx.status( 200 );
        } catch ( Exception e ) {
            ctx.status( 500 ).result( e.getMessage() );
        }
    }


    void removeQueryInterface( final Context ctx ) {
        String uniqueName = ctx.body();
        try {
            QueryInterfaceManager.getInstance().removeQueryInterface( Catalog.getInstance(), uniqueName );
            ctx.status( 200 );
        } catch ( RuntimeException e ) {
            log.error( "Could not remove query interface {}", ctx.body(), e );
            ctx.status( 500 ).result( e.getMessage() );
        }
    }


    /**
     * Get the required information for the uml view: Foreign keys, Tables with its columns
     */
    void getUml( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        List<ForeignKey> fKeys = new ArrayList<>();
        List<DbTable> tables = new ArrayList<>();

        LogicalRelSnapshot relSnapshot = Catalog.snapshot().rel();
        long namespaceId = request.namespaceId == null ? Catalog.defaultNamespaceId : request.namespaceId;
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( namespaceId ).orElseThrow();
        List<LogicalTable> entities = relSnapshot.getTablesFromNamespace( namespace.id );

        for ( LogicalTable table : entities ) {
            if ( table.entityType == EntityType.ENTITY || table.entityType == EntityType.SOURCE ) {
                // get foreign keys
                List<LogicalForeignKey> foreignKeys = Catalog.snapshot().rel().getForeignKeys( table.id );
                for ( LogicalForeignKey logicalForeignKey : foreignKeys ) {
                    for ( int i = 0; i < logicalForeignKey.getReferencedKeyFieldNames().size(); i++ ) {
                        fKeys.add( ForeignKey.builder()
                                .targetSchema( logicalForeignKey.getReferencedKeyNamespaceName() )
                                .targetTable( logicalForeignKey.getReferencedKeyEntityName() )
                                .targetColumn( logicalForeignKey.getReferencedKeyFieldNames().get( i ) )
                                .sourceSchema( logicalForeignKey.getSchemaName() )
                                .sourceTable( logicalForeignKey.getTableName() )
                                .sourceColumn( logicalForeignKey.getFieldNames().get( i ) )
                                .fkName( logicalForeignKey.name )
                                .onUpdate( logicalForeignKey.updateRule.toString() )
                                .onDelete( logicalForeignKey.deleteRule.toString() )
                                .build() );
                    }
                }

                // get tables with its columns
                DbTable dbTable = new DbTable( table.name, namespace.name, table.modifiable, table.entityType );

                for ( LogicalColumn column : relSnapshot.getColumns( table.id ) ) {
                    dbTable.addColumn( UiColumnDefinition.builder().name( column.name ).build() );
                }

                // get primary key with its columns
                if ( table.primaryKey != null ) {
                    LogicalPrimaryKey primaryKey = Catalog.snapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
                    for ( String columnName : primaryKey.getFieldNames() ) {
                        dbTable.addPrimaryKeyField( columnName );
                    }
                }

                // get unique constraints
                List<LogicalConstraint> logicalConstraints = Catalog.snapshot().rel().getConstraints( table.id );
                for ( LogicalConstraint logicalConstraint : logicalConstraints ) {
                    if ( logicalConstraint.type == ConstraintType.UNIQUE ) {
                        // TODO: unique constraints can be over multiple columns.
                        if ( logicalConstraint.key.getFieldNames().size() == 1 &&
                                logicalConstraint.key.getSchemaName().equals( dbTable.getSchema() ) &&
                                logicalConstraint.key.getTableName().equals( dbTable.getTableName() ) ) {
                            dbTable.addUniqueColumn( logicalConstraint.key.getFieldNames().get( 0 ) );
                        }
                        // table.addUnique( new ArrayList<>( catalogConstraint.key.columnNames ));
                    }
                }

                // get unique indexes
                List<LogicalIndex> logicalIndices = Catalog.snapshot().rel().getIndexes( table.id, true );
                for ( LogicalIndex logicalIndex : logicalIndices ) {
                    // TODO: unique indexes can be over multiple columns.
                    if ( logicalIndex.key.getFieldNames().size() == 1 &&
                            logicalIndex.key.getSchemaName().equals( dbTable.getSchema() ) &&
                            logicalIndex.key.getTableName().equals( dbTable.getTableName() ) ) {
                        dbTable.addUniqueColumn( logicalIndex.key.getFieldNames().get( 0 ) );
                    }
                    // table.addUnique( new ArrayList<>( catalogIndex.key.columnNames ));
                }

                tables.add( dbTable );
            }
        }

        ctx.json( new Uml( tables, fKeys ) );
    }


    /**
     * Add foreign key
     */
    void addForeignKey( final Context ctx ) {
        ForeignKey fk = ctx.bodyAsClass( ForeignKey.class );

        String[] t = fk.getSourceTable().split( "\\." );
        String fkTable = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        t = fk.getTargetTable().split( "\\." );
        String pkTable = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        String sql = String.format( "ALTER TABLE %s ADD CONSTRAINT \"%s\" FOREIGN KEY (\"%s\") REFERENCES %s(\"%s\") ON UPDATE %s ON DELETE %s",
                fkTable, fk.getFkName(), fk.getSourceColumn(), pkTable, fk.getTargetColumn(), fk.getOnUpdate(), fk.getOnDelete() );
        QueryLanguage language = QueryLanguage.from( "sql" );
        Result<?, ?> res = LanguageCrud.anyQueryResult(
                QueryContext.builder()
                        .query( sql )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );
        ctx.json( res );
    }


    /**
     * Create or drop a namespace
     */
    void namespaceRequest( final Context ctx ) {
        Namespace namespace = ctx.bodyAsClass( Namespace.class );

        if ( namespace.getType() == DataModel.GRAPH ) {
            createGraph( namespace, ctx );
            return;
        }

        DataModel type = namespace.getType();

        // create namespace
        if ( namespace.isCreate() && !namespace.isDrop() ) {

            StringBuilder query = new StringBuilder( "CREATE " );
            if ( Objects.requireNonNull( namespace.getType() ) == DataModel.DOCUMENT ) {
                query.append( "DOCUMENT " );
            }

            query.append( "NAMESPACE " );

            query.append( "\"" ).append( namespace.getName() ).append( "\"" );
            if ( namespace.getAuthorization() != null && !namespace.getAuthorization().isEmpty() ) {
                query.append( " AUTHORIZATION " ).append( namespace.getAuthorization() );
            }
            QueryLanguage language = QueryLanguage.from( "sql" );
            Result<?, ?> res = LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query.toString() )
                            .language( language )
                            .origin( ORIGIN )
                            .transactionManager( transactionManager )
                            .build(), UIRequest.builder().build() ).get( 0 );
            ctx.json( res );
        }
        // drop namespace
        else if ( !namespace.isCreate() && namespace.isDrop() ) {
            if ( type == null ) {
                List<LogicalNamespace> namespaces = Catalog.snapshot().getNamespaces( new org.polypheny.db.catalog.logistic.Pattern( namespace.getName() ) );
                assert namespaces.size() == 1;
            }

            StringBuilder query = new StringBuilder( "DROP NAMESPACE " );
            query.append( "\"" ).append( namespace.getName() ).append( "\"" );
            if ( namespace.isCascade() ) {
                query.append( " CASCADE" );
            }
            QueryLanguage language = QueryLanguage.from( "sql" );
            Result<?, ?> res = LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query.toString() )
                            .language( language )
                            .origin( ORIGIN )
                            .transactionManager( transactionManager )
                            .build(), UIRequest.builder().build() ).get( 0 );
            ctx.json( res );
        } else {
            ctx.json( RelationalResult.builder().error( "Neither the field 'create' nor the field 'drop' was set." ).build() );
        }
    }


    private void createGraph( Namespace namespace, Context ctx ) {
        QueryLanguage cypher = QueryLanguage.from( "cypher" );
        QueryContext context = QueryContext.builder()
                .query( "CREATE DATABASE " + namespace.getName() + " ON STORE " + namespace.getStore() )
                .language( cypher )
                .origin( ORIGIN )
                .transactionManager( transactionManager )
                .build();
        ctx.json( LanguageCrud.anyQueryResult( context, UIRequest.builder().build() ).get( 0 ) );
    }


    /**
     * Get all supported data types of the DBMS.
     */
    public void getTypeInfo( final Context ctx ) {
        ctx.json( PolyType.allowedFieldTypes().stream().map( PolyTypeModel::from ).toList() );
    }


    /**
     * Get available actions for foreign key constraints
     */
    void getForeignKeyActions( final Context ctx ) {
        ForeignKeyOption[] options = ForeignKeyOption.values();
        String[] arr = new String[options.length];
        for ( int i = 0; i < options.length; i++ ) {
            arr[i] = options[i].name();
        }
        ctx.json( arr );
    }


    /**
     * Send updates to the UI if Information objects in the query analyzer change.
     */
    @Override
    public void observeInfos( final String infoAsJson, final String analyzerId, final Session session ) {
        WebSocket.sendMessage( session, infoAsJson );
    }


    /**
     * Send an updated pageList of the query analyzer to the UI.
     */
    @Override
    public void observePageList( final InformationPage[] pages, final String analyzerId, final Session session ) {
        List<SidebarElement> nodes = new ArrayList<>();
        for ( InformationPage page : pages ) {
            nodes.add( new SidebarElement( page.getId(), page.getName(), DataModel.RELATIONAL, analyzerId + "/", page.getIcon() ).setLabel( page.getLabel() ) );
        }
        WebSocket.sendMessage( session, gson.toJson( nodes.toArray( new SidebarElement[0] ) ) );
    }


    /**
     * Get the content of an InformationPage of a query analyzer.
     */
    public void getAnalyzerPage( final Context ctx ) {
        String[] params = ctx.bodyAsClass( String[].class );
        ctx.json( InformationManager.getInstance( params[0] ).getPage( params[1] ) );
    }


    void getFile( final Context ctx ) {
        getFile( ctx, "tmp", true );
    }


    private File getFile( Context ctx, String location, boolean sendBack ) {
        String fileName = ctx.pathParam( "file" );
        File folder = PolyphenyHomeDirManager.getInstance().registerNewFolder( location );
        File f = PolyphenyHomeDirManager.getInstance().registerNewFile( folder, fileName );
        if ( !f.exists() ) {
            ctx.status( 404 );
            ctx.result( "" );
            return f;
        } else if ( f.isDirectory() ) {
            getDirectory( f, ctx );
        }
        ContentInfoUtil util = new ContentInfoUtil();
        ContentInfo info = null;
        try {
            info = util.findMatch( f );
        } catch ( IOException ignored ) {
        }
        if ( info != null && info.getMimeType() != null ) {
            ctx.contentType( info.getMimeType() );
        } else {
            ctx.contentType( "application/octet-stream" );
        }
        if ( info != null && info.getFileExtensions() != null && info.getFileExtensions().length > 0 ) {
            ctx.header( "Content-Disposition", "attachment; filename=" + "file." + info.getFileExtensions()[0] );
        } else {
            ctx.header( "Content-Disposition", "attachment; filename=" + "file" );
        }
        long fileLength = f.length();
        String range = ctx.req().getHeader( "Range" );
        if ( range != null ) {
            long rangeStart = 0;
            long rangeEnd = 0;
            Pattern pattern = Pattern.compile( "bytes=(\\d*)-(\\d*)" );
            Matcher m = pattern.matcher( range );
            if ( m.find() && m.groupCount() == 2 ) {
                rangeStart = Long.parseLong( m.group( 1 ) );
                String group2 = m.group( 2 );
                //chrome and firefox send "bytes=0-"
                //safari sends "bytes=0-1" to get the file length and then bytes=0-fileLength
                if ( group2 != null && !group2.isEmpty() ) {
                    rangeEnd = Long.parseLong( group2 );
                } else {
                    rangeEnd = Math.min( rangeStart + 10_000_000L, fileLength - 1 );
                }
                if ( rangeEnd >= fileLength ) {
                    ctx
                            .status( 416 )//range not satisfiable
                            .result( "" );
                }
            } else {
                ctx
                        .status( 416 )//range not satisfiable
                        .json( "" );
            }
            try {
                //see https://github.com/dessalines/torrenttunes-client/blob/master/src/main/java/com/torrenttunes/client/webservice/Platform.java
                ctx.res().setHeader( "Accept-Ranges", "bytes" );
                ctx.status( 206 );//partial content
                int len = Long.valueOf( rangeEnd - rangeStart ).intValue() + 1;
                ctx.res().setHeader( "Content-Range", String.format( "bytes %d-%d/%d", rangeStart, rangeEnd, fileLength ) );

                RandomAccessFile raf = new RandomAccessFile( f, "r" );
                raf.seek( rangeStart );
                ServletOutputStream os = ctx.res().getOutputStream();
                byte[] buf = new byte[256];
                while ( len > 0 ) {
                    int read = raf.read( buf, 0, Math.min( buf.length, len ) );
                    os.write( buf, 0, read );
                    len -= read;
                }
                os.flush();
                os.close();
                raf.close();
            } catch ( IOException ignored ) {
                ctx.status( 500 );
            }
        } else {
            if ( sendBack ) {
                ctx.res().setContentLengthLong( (int) fileLength );
                try ( FileInputStream fis = new FileInputStream( f ); ServletOutputStream os = ctx.res().getOutputStream() ) {
                    IOUtils.copyLarge( fis, os );
                    os.flush();
                } catch ( IOException ignored ) {
                    ctx.status( 500 );
                }
            }
        }
        ctx.result( "" );

        return f;
    }


    void getDirectory( File dir, Context ctx ) {
        ctx.header( "Content-ExpressionType", "application/zip" );
        ctx.header( "Content-Disposition", "attachment; filename=" + dir.getName() + ".zip" );
        String zipFileName = UUID.randomUUID() + ".zip";
        File zipFile = new File( System.getProperty( "user.home" ), ".polypheny/tmp/" + zipFileName );
        try ( ZipOutputStream zipOut = new ZipOutputStream( Files.newOutputStream( zipFile.toPath() ) ) ) {
            zipDirectory( "", dir, zipOut );
        } catch ( IOException e ) {
            ctx.status( 500 );
            log.error( "Could not zip directory", e );
        }
        ctx.res().setContentLengthLong( zipFile.length() );
        try ( OutputStream os = ctx.res().getOutputStream(); InputStream is = new FileInputStream( zipFile ) ) {
            IOUtils.copy( is, os );
        } catch ( IOException e ) {
            log.error( "Could not write zipOutputStream to response", e );
            ctx.status( 500 );
        }
        zipFile.delete();
        ctx.result( "" );
    }


    void getCatalog( final Context ctx ) {
        // Assigning the result to a variable causes an error when the switch expression is not exhaustive
        Context ignore = switch ( Catalog.mode ) {
            case PRODUCTION -> ctx.status( HttpStatus.FORBIDDEN ).result( "Forbidden" );
            case DEVELOPMENT, BENCHMARK, TEST -> ctx.json( Catalog.getInstance().getJson() );
        };
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    /**
     * Get the Number of rows in a table
     */
    private long getTableSize( Transaction transaction, final UIRequest request ) {
        String tableId = getFullEntityName( request.entityId );
        String query = "SELECT count(*) FROM " + tableId;
        if ( request.filter != null ) {
            query += " " + filterTable( request.filter );
        }

        QueryLanguage language = QueryLanguage.from( "sql" );
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager ).build(), transaction ).get( 0 );
        List<List<PolyValue>> values = context.execute( context.getStatement() ).getIterator().getNextBatch();
        // We expect the result to be in the first column of the first row
        if ( values.isEmpty() || values.get( 0 ).isEmpty() ) {
            return 0;
        } else {
            PolyNumber number = values.get( 0 ).get( 0 ).asNumber();
            if ( context.getStatement().getMonitoringEvent() != null ) {
                StatementEvent eventData = context.getStatement().getMonitoringEvent();
                eventData.setRowCount( number.longValue() );
            }
            return number.longValue();
        }
    }


    /**
     * Get the number of rows that should be displayed in one page in the data view
     */
    public int getPageSize() {
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }


    private String filterTable( final Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", " WHERE ", "" );
        int counter = 0;
        for ( Map.Entry<String, String> entry : filter.entrySet() ) {
            //special treatment for arrays
            if ( entry.getValue().startsWith( "[" ) ) {
                joiner.add( "\"" + entry.getKey() + "\"" + " = ARRAY" + entry.getValue() );
                counter++;
            }
            //default
            else if ( !entry.getValue().isEmpty() ) {
                joiner.add( "CAST (\"" + entry.getKey() + "\" AS VARCHAR(8000)) LIKE '" + entry.getValue() + "%'" );
                counter++;
            }
        }
        String out = "";
        if ( counter > 0 ) {
            out = joiner.toString();
        }
        return out;
    }


    /**
     * Generates the ORDER BY clause of a query if a sorted column is requested by the UI
     */
    private String sortTable( final Map<String, SortState> sorting ) {
        StringJoiner joiner = new StringJoiner( ",", " ORDER BY ", "" );
        int counter = 0;
        for ( Map.Entry<String, SortState> entry : sorting.entrySet() ) {
            if ( entry.getValue().sorting ) {
                joiner.add( "\"" + entry.getKey() + "\" " + entry.getValue().direction );
                counter++;
            }
        }
        String out = "";
        if ( counter > 0 ) {
            out = joiner.toString();
        }
        return out;
    }


    public Transaction getTransaction() {
        return getTransaction( true, this );
    }


    public static Transaction getTransaction( boolean useCache, TransactionManager transactionManager, long userId, long databaseId ) {
        return getTransaction( useCache, transactionManager, userId, databaseId, ORIGIN );
    }


    public static Transaction getTransaction( boolean useCache, TransactionManager transactionManager, long userId, long namespaceId, String origin ) {
        Transaction transaction = transactionManager.startTransaction(
                userId,
                namespaceId,
                null,
                origin,
                MultimediaFlavor.FILE );
        transaction.setUseCache( useCache );
        return transaction;
    }


    public static Transaction getTransaction( boolean useCache, Crud crud ) {
        return getTransaction( useCache, crud.transactionManager, Catalog.defaultUserId, Catalog.defaultNamespaceId );
    }


    public void getPolyAlgRegistry( Context ctx ) {
        ctx.json( PolyAlgRegistry.serialize() );
    }


    /**
     * @return a serialized version of the plan built from the given polyAlgRequest
     * @throws NodeParseException if the parser is not able to construct the intermediary PolyAlgNode tree
     * @throws RuntimeException if polyAlg cannot be parsed into a valid AlgNode tree
     */
    public void buildPlanFromPolyAlg( final Context ctx ) {
        PolyAlgRequest request = ctx.bodyAsClass( PolyAlgRequest.class );
        try {
            AlgNode node = PolyPlanBuilder.buildFromPolyAlg( request.polyAlg, request.planType ).alg;
            ctx.json( node.serializePolyAlgebra( new ObjectMapper() ) );
        } catch ( Exception e ) {
            //e.printStackTrace();
            ctx.json( Map.of( "errorMsg", e.getMessage() ) );
            ctx.status( 400 );
        }
    }


    void createDockerInstance( final Context ctx ) {
        try {
            CreateDockerRequest req = ctx.bodyAsClass( CreateDockerRequest.class );
            Optional<HandshakeInfo> res = DockerSetupHelper.newDockerInstance(
                    req.hostname(),
                    req.alias(),
                    req.registry(),
                    req.communicationPort(),
                    req.handshakePort(),
                    req.proxyPort(),
                    true
            );

            ctx.json( new CreateDockerResponse( res.orElse( null ), DockerManager.getInstance().getDockerInstancesMap() ) );
        } catch (
                DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }


    }


    void getDockerInstances( final Context ctx ) {
        ctx.json( DockerManager.getInstance().getDockerInstancesMap() );
    }


    void getDockerInstance( final Context ctx ) {
        try {
            int dockerId = Integer.parseInt( ctx.pathParam( "dockerId" ) );

            ctx.json( DockerManager.getInstance().getInstanceById( dockerId ).map( DockerInstance::getInfo ).orElseThrow( () -> new DockerUserException( 404, "No Docker instance with that id" ) ) );
        } catch ( NumberFormatException e ) {
            ctx.status( HttpStatus.BAD_REQUEST ).result( "Malformed dockerId value" );
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void updateDockerInstance( final Context ctx ) {
        UpdateDockerRequest request = ctx.bodyAsClass( UpdateDockerRequest.class );

        try {
            ctx.json( DockerSetupHelper.updateDockerInstance( request.id(), request.hostname(), request.alias(), request.registry() ) );
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void reconnectToDockerInstance( final Context ctx ) {
        try {
            ctx.json( DockerSetupHelper.reconnectToInstance( Integer.parseInt( ctx.pathParam( "dockerId" ) ) ) );
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void pingDockerInstance( final Context ctx ) {
        try {
            DockerManager.getInstance().getInstanceById( Integer.parseInt( ctx.pathParam( "dockerId" ) ) ).orElseThrow( () -> new DockerUserException( 404, "No instance with that id" ) ).ping();
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void deleteDockerInstance( final Context ctx ) {
        try {
            DockerSetupHelper.removeDockerInstance( Integer.parseInt( ctx.pathParam( "dockerId" ) ) );

            ctx.json( new InstancesAndAutoDocker( DockerManager.getInstance().getDockerInstancesMap(), AutoDocker.getInstance().getStatus() ) );
        } catch ( NumberFormatException e ) {
            ctx.status( HttpStatus.BAD_REQUEST ).result( "Malformed id value" );
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void getAutoDockerStatus( final Context ctx ) {
        ctx.json( AutoDocker.getInstance().getStatus() );
    }


    void doAutoHandshake( final Context ctx ) {
        try {
            AutoDocker.getInstance().doAutoConnect();
            ctx.json(
                    new AutoDockerResult(
                            AutoDocker.getInstance().getStatus(),
                            DockerManager.getInstance().getDockerInstancesMap()
                    )
            );
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void getHandshakes( final Context ctx ) {
        ctx.json( HandshakeManager.getInstance().getActiveHandshakes() );
    }


    void getHandshake( final Context ctx ) {
        long id = Long.parseLong( ctx.pathParam( "id" ) );
        Optional<HandshakeInfo> maybeHandshake = HandshakeManager.getInstance().getHandshake( id );
        if ( maybeHandshake.isPresent() ) {
            ctx.json( maybeHandshake.get() );
        } else {
            ctx.status( 404 ).result( "No handshake with that id" );
        }
    }


    void restartHandshake( final Context ctx ) {
        try {
            ctx.json( HandshakeManager.getInstance().restartHandshake( Long.parseLong( ctx.pathParam( "id" ) ) ) );
        } catch ( DockerUserException e ) {
            ctx.status( e.getStatus() ).result( e.getMessage() );
        }
    }


    void cancelHandshake( final Context ctx ) {
        long id = Long.parseLong( ctx.pathParam( "id" ) );
        if ( HandshakeManager.getInstance().cancelAndRemoveHandshake( id ) ) {
            ctx.status( 200 );
        } else {
            ctx.status( 404 );
        }
    }


    void deleteHandshake( final Context ctx ) {
        long id = Long.parseLong( ctx.pathParam( "id" ) );
        if ( HandshakeManager.getInstance().cancelAndRemoveHandshake( id ) ) {
            ctx.status( 200 ).json( HandshakeManager.getInstance().getActiveHandshakes() );
        } else {
            ctx.status( 404 );
        }
    }


    void getDockerSettings( final Context ctx ) {
        ctx.json(
                new DockerSettings( RuntimeConfig.DOCKER_CONTAINER_REGISTRY.getString() )
        );
    }


    void updateDockerSettings( final Context ctx ) {
        DockerSettings settings = ctx.bodyAsClass( DockerSettings.class );
        if ( settings.defaultRegistry() != null ) {
            RuntimeConfig.DOCKER_CONTAINER_REGISTRY.setString( settings.defaultRegistry() );
        }
        getDockerSettings( ctx );
    }


    /**
     * Loads the plugin in the supplied path.
     */
    public void loadPlugins( final Context ctx ) {
        ctx.uploadedFiles( "plugins" ).forEach( file -> {
            String[] splits = file.filename().split( "/" );
            String normalizedFileName = splits[splits.length - 1];
            splits = normalizedFileName.split( "\\\\" );
            normalizedFileName = splits[splits.length - 1];
            File f = new File( System.getProperty( "user.home" ), ".polypheny/plugins/" + normalizedFileName );
            try {
                FileUtils.copyInputStreamToFile( file.content(), f );
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
            PolyPluginManager.loadAdditionalPlugin( f );
        } );

    }


    /**
     * Unload the plugin with the supplied pluginId.
     */
    public void unloadPlugin( final Context ctx ) {
        String pluginId = ctx.bodyAsClass( String.class );

        ctx.json( PolyPluginManager.unloadAdditionalPlugin( pluginId ) );
    }


    /**
     * Helper method to zip a directory
     */
    private static void zipDirectory( String basePath, File dir, ZipOutputStream zipOut ) throws IOException {
        byte[] buffer = new byte[4096];
        File[] files = dir.listFiles();
        assert files != null;
        for ( File file : files ) {
            if ( file.isDirectory() ) {
                String path = basePath + file.getName() + "/";
                zipOut.putNextEntry( new ZipEntry( path ) );
                zipDirectory( path, file, zipOut );
                zipOut.closeEntry();
            } else {
                FileInputStream fin = new FileInputStream( file );
                zipOut.putNextEntry( new ZipEntry( basePath + file.getName() ) );
                int length;
                while ( (length = fin.read( buffer )) > 0 ) {
                    zipOut.write( buffer, 0, length );
                }
                zipOut.closeEntry();
                fin.close();
            }
        }
    }


    public void getAvailablePlugins( Context ctx ) {
        ctx.json( PolyPluginManager
                .getPLUGINS()
                .values()
                .stream()
                .map( PluginStatus::from )
                .toList() );
    }


    @Override
    public void propertyChange( PropertyChangeEvent evt ) {
        authCrud.broadcast( SnapshotModel.from( Catalog.snapshot() ) );
    }


}
