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

package org.polypheny.db.webui;


import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import io.javalin.http.Context;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Part;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSettingDirectory;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.FunctionalIndexInfo;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.NameGenerator;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.docker.AutoDocker;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerSetupHelper;
import org.polypheny.db.docker.DockerSetupHelper.DockerReconnectResult;
import org.polypheny.db.docker.DockerSetupHelper.DockerSetupResult;
import org.polypheny.db.docker.DockerSetupHelper.DockerUpdateResult;
import org.polypheny.db.docker.HandshakeManager;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceInformation;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceInformationRequest;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.languages.LanguageManager;
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
import org.polypheny.db.webui.models.PlacementModel;
import org.polypheny.db.webui.models.PlacementModel.RelationalStore;
import org.polypheny.db.webui.models.QueryInterfaceModel;
import org.polypheny.db.webui.models.SidebarElement;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.TableConstraint;
import org.polypheny.db.webui.models.Uml;
import org.polypheny.db.webui.models.UnderlyingTables;
import org.polypheny.db.webui.models.catalog.AdapterModel;
import org.polypheny.db.webui.models.catalog.AdapterModel.AdapterSettingValueModel;
import org.polypheny.db.webui.models.catalog.PolyTypeModel;
import org.polypheny.db.webui.models.catalog.SnapshotModel;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition;
import org.polypheny.db.webui.models.requests.AlgRequest;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest.Update;
import org.polypheny.db.webui.models.requests.ColumnRequest;
import org.polypheny.db.webui.models.requests.ConstraintRequest;
import org.polypheny.db.webui.models.requests.EditTableRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest.ModifyPartitionRequest;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.RelationalResult.RelationalResultBuilder;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.webui.models.results.Result.ResultBuilder;
import org.polypheny.db.webui.models.results.ResultType;


@Getter
@Slf4j
public class Crud implements InformationObserver, PropertyChangeListener {

    private static final Gson gson = new Gson();
    public static final String ORIGIN = "Polypheny-UI";
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
                        .origin( transaction.getOrigin() )
                        .batch( request.noLimit ? -1 : getPageSize() )
                        .transactionManager( transactionManager )
                        .build(), transaction ).get( 0 );
        resultBuilder = (RelationalResultBuilder<?, ?>) builder.apply( implementationContext.execute( implementationContext.getStatement() ), request, implementationContext.getStatement() );

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
            log.error( "Caught exception while committing transaction", e );
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
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
        IndexModel table = ctx.bodyAsClass( IndexModel.class );
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" RENAME TO \"%s\"", table.getNamespaceId(), table.getEntityId(), table.getName() );
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
                Part part = ctx.req.getPart( logicalColumn.name );
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
    public static void attachQueryAnalyzer( InformationManager queryAnalyzer, long executionTime, String commitStatus, int numberOfQueries ) {
        InformationPage p1 = new InformationPage( "Transaction", "Analysis of the transaction." );
        queryAnalyzer.addPage( p1 );
        InformationGroup g1 = new InformationGroup( p1, "Execution time" );
        queryAnalyzer.addGroup( g1 );
        InformationText text1;
        if ( executionTime < 1e4 ) {
            text1 = new InformationText( g1, String.format( "Execution time: %d nanoseconds", executionTime ) );
        } else {
            long millis = TimeUnit.MILLISECONDS.convert( executionTime, TimeUnit.NANOSECONDS );
            // format time: see: https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java#answer-625444
            DateFormat df = new SimpleDateFormat( "m 'min' s 'sec' S 'ms'" );
            String durationText = df.format( new Date( millis ) );
            text1 = new InformationText( g1, String.format( "Execution time: %s", durationText ) );
        }
        queryAnalyzer.registerInformation( text1 );

        // Number of queries
        InformationGroup g2 = new InformationGroup( p1, "Number of queries" );
        queryAnalyzer.addGroup( g2 );
        InformationText text2 = new InformationText( g2, String.format( "Number of queries in this transaction: %d", numberOfQueries ) );
        queryAnalyzer.registerInformation( text2 );

        // Commit Status
        InformationGroup g3 = new InformationGroup( p1, "Status" );
        queryAnalyzer.addGroup( g3 );
        InformationText text3 = new InformationText( g3, commitStatus );
        queryAnalyzer.registerInformation( text3 );
    }


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
        Transaction transaction = getTransaction();

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

        if ( result.error == null && statisticCrud.isActiveTracking() ) {
            transaction.addChangedTable( tableId );
        }
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
            String _oldValues = new BufferedReader( new InputStreamReader( ctx.req.getPart( "oldValues" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
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
            Part part = ctx.req.getPart( logicalColumn.name );
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
                        .language( language )
                        .origin( ORIGIN )
                        .transactionManager( transactionManager )
                        .build(), UIRequest.builder().build() ).get( 0 );

        if ( result.error == null && result.data.length == 1 && statisticCrud.isActiveTracking() ) {
            transaction.addChangedTable( fullName );
        }
        ctx.json( result );
    }


    void batchUpdate( final Context ctx ) throws ServletException, IOException {
        ctx.contentType( "multipart/form-data" );
        initMultipart( ctx );
        BatchUpdateRequest request;

        String jsonRequest = new BufferedReader( new InputStreamReader( ctx.req.getPart( "request" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
        request = gson.fromJson( jsonRequest, BatchUpdateRequest.class );

        Transaction transaction = getTransaction();
        Statement statement;
        QueryLanguage language = QueryLanguage.from( "sql" );
        List<Result<?, ?>> results = new ArrayList<>();
        for ( Update update : request.updates ) {
            statement = transaction.createStatement();
            String query = update.getQuery( request.tableId, statement, ctx.req );

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
                for ( Entry<String, List<ExportedColumn>> entry : dataSource.getExportedColumns().entrySet() ) {
                    List<UiColumnDefinition> columnList = new ArrayList<>();
                    for ( ExportedColumn col : entry.getValue() ) {
                        UiColumnDefinition dbCol = UiColumnDefinition.builder()
                                .name( col.name )
                                .dataType( col.type.getName() )
                                .collectionsType( col.collectionsType == null ? "" : col.collectionsType.getName() )
                                .nullable( col.nullable )
                                .precision( col.length )
                                .scale( col.scale )
                                .dimension( col.dimension )
                                .cardinality( col.cardinality )
                                .primary( col.primary )
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
            if ( oldColumn.defaultValue == null || newColumn.defaultValue == null || !oldColumn.defaultValue.equals( newColumn.defaultValue ) ) {
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
        }


    }


    /**
     * Add a column to an existing table
     */
    void addColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );

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

        String[] t = request.table.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        String query;
        if ( request.constraint.type.equals( "PRIMARY KEY" ) ) {
            query = String.format( "ALTER TABLE %s DROP PRIMARY KEY", tableId );
        } else if ( request.constraint.type.equals( "FOREIGN KEY" ) ) {
            query = String.format( "ALTER TABLE %s DROP FOREIGN KEY \"%s\"", tableId, request.constraint.name );
        } else {
            query = String.format( "ALTER TABLE %s DROP CONSTRAINT \"%s\"", tableId, request.constraint.name );
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
     * Add a primary key to a table
     */
    void addPrimaryKey( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );

        String[] t = request.table.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

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

        String[] t = request.table.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result<?, ?> result;
        if ( request.constraint.columns.length > 0 ) {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( String s : request.constraint.columns ) {
                joiner.add( "\"" + s + "\"" );
            }
            String query = "ALTER TABLE " + tableId + " ADD CONSTRAINT \"" + request.constraint.name + "\" UNIQUE " + joiner;
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
            ImmutableMap<Long, List<Long>> underlyingTableOriginal = table.unwrap( LogicalView.class ).orElseThrow().underlyingTables;
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
        Snapshot snapshot = Catalog.getInstance().getSnapshot();

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
        IndexModel index = ctx.bodyAsClass( IndexModel.class );
        if ( !index.getMethod().equalsIgnoreCase( "ADD" ) && !index.getMethod().equalsIgnoreCase( "DROP" ) && !index.getMethod().equalsIgnoreCase( "MODIFY" ) ) {
            ctx.json( RelationalResult.builder().error( "Invalid request" ).build() );
            return;
        }
        StringJoiner columnJoiner = new StringJoiner( ",", "(", ")" );
        int counter = 0;
        if ( !index.getMethod().equalsIgnoreCase( "DROP" ) ) {
            for ( long col : index.columnIds ) {
                columnJoiner.add( "\"" + Catalog.snapshot().rel().getColumn( col ).orElseThrow().name + "\"" );
                counter++;
            }
        }
        String columnListStr = counter > 0 ? columnJoiner.toString() : "";
        String query = String.format(
                "ALTER TABLE \"%s\".\"%s\" %s PLACEMENT %s ON STORE \"%s\"",
                index.getNamespaceId(),
                index.getEntityId(),
                index.getMethod().toUpperCase(),
                columnListStr,
                index.getStoreUniqueName() );
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

                String defaultValue = currentColumn.getDefaultValue();

                // Used specifically for Temp-Partitioning since number of selected partitions remains 2 but chunks change
                // enables user to use selected "number of partitions" being used as default value for "number of internal data chunks"
                if ( request.method.equals( PartitionType.TEMPERATURE ) ) {

                    if ( type.equals( FieldType.STRING ) && currentColumn.getDefaultValue().equals( "-04071993" ) ) {
                        defaultValue = String.valueOf( request.numPartitions );
                    }
                }

                constructedRow.add( new PartitionFunctionColumn( type, defaultValue )
                        .setModifiable( currentColumn.isModifiable() )
                        .setMandatory( currentColumn.isMandatory() )
                        .setSqlPrefix( currentColumn.getSqlPrefix() )
                        .setSqlSuffix( currentColumn.getSqlSuffix() ) );
            }
        }

        return constructedRow;
    }


    void getPartitionFunctionModel( final Context ctx ) {
        PartitioningRequest request = ctx.bodyAsClass( PartitioningRequest.class );

        // Get correct partition function
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( request.method );

        // Check whether the selected partition function supports the selected partition column
        LogicalColumn partitionColumn;

        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace( request.schemaName ).orElseThrow();

        partitionColumn = Catalog.getInstance().getSnapshot().rel().getColumn( namespace.id, request.tableName, request.column ).orElseThrow();

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
        //see https://stackoverflow.com/questions/16872492/gson-and-abstract-superclasses-deserialization-issue
        JsonDeserializer<Adapter<?>> storeDeserializer = ( json, typeOfT, context ) -> {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get( "type" ).getAsString();
            try {
                return context.deserialize( jsonObject, Class.forName( type ) );
            } catch ( ClassNotFoundException cnfe ) {
                throw new JsonParseException( "Unknown element type: " + type, cnfe );
            }
        };
        Gson adapterGson = new GsonBuilder().registerTypeAdapter( Adapter.class, storeDeserializer ).create();
        Adapter<?> adapter = adapterGson.fromJson( ctx.body(), Adapter.class );
        try {
            if ( adapter instanceof DataStore ) {
                AdapterManager.getInstance().getStore( adapter.getAdapterId() ).orElseThrow().updateSettings( adapter.getCurrentSettings() );
            } else if ( adapter instanceof DataSource ) {
                AdapterManager.getInstance().getSource( adapter.getAdapterId() ).orElseThrow().updateSettings( adapter.getCurrentSettings() );
            }
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
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
            ctx.json( RelationalResult.builder().error( "Error while resetting caches: " + e.getMessage() ).build() );
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
    void addAdapter( final Context ctx ) throws ServletException, IOException {
        initMultipart( ctx );
        String body = "";
        Map<String, InputStream> inputStreams = new HashMap<>();

        // collect all files e.g. csv files
        for ( Part part : ctx.req.getParts() ) {
            if ( part.getName().equals( "body" ) ) {
                body = IOUtils.toString( ctx.req.getPart( "body" ).getInputStream(), StandardCharsets.UTF_8 );
            } else {
                inputStreams.put( part.getName(), part.getInputStream() );
            }
        }

        AdapterModel a = HttpServer.mapper.readValue( body, AdapterModel.class );
        Map<String, String> settings = new HashMap<>();

        ConnectionMethod method = ConnectionMethod.UPLOAD;
        if ( a.settings.containsKey( "method" ) ) {
            method = ConnectionMethod.valueOf( a.settings.get( "method" ).value().toUpperCase() );
        }
        AdapterTemplate adapter = AdapterManager.getAdapterTemplate( a.adapterName, a.type );
        Map<String, AbstractAdapterSetting> allSettings = adapter.settings.stream().collect( Collectors.toMap( e -> e.name, e -> e ) );

        for ( AdapterSettingValueModel entry : a.settings.values() ) {
            AbstractAdapterSetting set = allSettings.get( entry.name() );
            if ( set == null ) {
                continue;
            }
            if ( set instanceof AbstractAdapterSettingDirectory setting ) {
                if ( method == ConnectionMethod.LINK ) {
                    Exception e = handleLinkFiles( ctx, a, setting, allSettings );
                    if ( e != null ) {
                        ctx.json( RelationalResult.builder().exception( e ).build() );
                        return;
                    }
                    settings.put( set.name, entry.value() );
                } else {
                    List<String> fileNames = HttpServer.mapper.readValue( entry.value(), new TypeReference<>() {
                    } );
                    String directory = handleUploadFiles( inputStreams, fileNames, setting, a );
                    settings.put( set.name, directory );
                }


            } else {
                settings.put( set.name, entry.value() );
            }
        }

        String query = String.format( "ALTER ADAPTERS ADD \"%s\" USING '%s' AS '%s' WITH '%s'", a.name, a.adapterName, a.type, Crud.gson.toJson( settings ) );
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
        UUID uuid = SecurityManager.getInstance().requestPathAccess( request.getName(), ctx.req.getSession().getId(), Path.of( request.getDirectoryName() ) );
        if ( uuid != null ) {
            ctx.json( uuid );
        } else {
            ctx.result( "" );
        }
    }


    private Exception handleLinkFiles( Context ctx, AdapterModel a, AbstractAdapterSettingDirectory setting, Map<String, AbstractAdapterSetting> settings ) {
        if ( !settings.containsKey( "directoryName" ) ) {
            return new GenericRuntimeException( "Security check for access was not performed; id missing." );
        }
        Path path = Path.of( settings.get( "directoryName" ).defaultValue );
        if ( !SecurityManager.getInstance().checkPathAccess( path ) ) {
            return new GenericRuntimeException( "Security check for access was not successful; not enough permissions." );
        }

        return null;
    }


    private static String handleUploadFiles( Map<String, InputStream> inputStreams, List<String> fileNames, AbstractAdapterSettingDirectory setting, AdapterModel a ) {
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
        List<QueryInterfaceInformation> interfaces = qim.getAvailableQueryInterfaceTypes();
        ctx.result( QueryInterfaceInformation.toJson( interfaces.toArray( new QueryInterfaceInformation[0] ) ) );
    }


    void addQueryInterface( final Context ctx ) {
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        QueryInterfaceInformationRequest request = ctx.bodyAsClass( QueryInterfaceInformationRequest.class );
        String generatedQuery = String.format( "ALTER INTERFACES ADD \"%s\" USING '%s' WITH '%s'", request.uniqueName, request.clazzName, gson.toJson( request.currentSettings ) );
        try {
            qim.addQueryInterface( Catalog.getInstance(), request.clazzName, request.uniqueName, request.currentSettings );
            ctx.json( RelationalResult.builder().affectedTuples( 1 ).query( generatedQuery ).build() );
        } catch ( RuntimeException e ) {
            log.error( "Exception while deploying query interface", e );
            ctx.json( RelationalResult.builder().error( e.getMessage() ).query( generatedQuery ).build() );
        }
    }


    void updateQueryInterfaceSettings( final Context ctx ) {
        QueryInterfaceModel request = ctx.bodyAsClass( QueryInterfaceModel.class );
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        try {
            qim.getQueryInterface( request.uniqueName ).updateSettings( request.currentSettings );
            ctx.json( RelationalResult.builder().affectedTuples( 1 ).build() );
        } catch ( Exception e ) {
            ctx.json( RelationalResult.builder().error( e.getMessage() ).build() );
        }
    }


    void removeQueryInterface( final Context ctx ) {
        String uniqueName = ctx.body();
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        String generatedQuery = String.format( "ALTER INTERFACES DROP \"%s\"", uniqueName );
        try {
            qim.removeQueryInterface( Catalog.getInstance(), uniqueName );
            ctx.json( RelationalResult.builder().affectedTuples( 1 ).query( generatedQuery ).build() );
        } catch ( RuntimeException e ) {
            log.error( "Could not remove query interface {}", ctx.body(), e );
            ctx.json( RelationalResult.builder().error( e.getMessage() ).query( generatedQuery ).build() );
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


    // helper for relAlg materialized View
    private TimeUnit getFreshnessType( String freshnessId ) {
        TimeUnit timeUnit;
        switch ( freshnessId ) {
            case "min":
            case "minutes":
                timeUnit = TimeUnit.MINUTES;
                break;
            case "hours":
                timeUnit = TimeUnit.HOURS;
                break;
            case "sec":
            case "seconds":
                timeUnit = TimeUnit.SECONDS;
                break;
            case "days":
            case "day":
                timeUnit = TimeUnit.DAYS;
                break;
            case "millisec":
            case "milliseconds":
                timeUnit = TimeUnit.MILLISECONDS;
                break;
            default:
                timeUnit = TimeUnit.MINUTES;
                break;
        }
        return timeUnit;
    }


    /**
     * Execute a logical plan coming from the Web-Ui plan builder
     */
    RelationalResult executeAlg( final AlgRequest request, Session session ) {
        Transaction transaction = getTransaction( request.analyze, request.useCache, this );
        transaction.getQueryAnalyzer().setSession( session );

        Statement statement = transaction.createStatement();
        long executionTime = 0;
        long temp = 0;

        InformationManager queryAnalyzer = transaction.getQueryAnalyzer().observe( this );

        AlgNode result;
        try {
            temp = System.nanoTime();
            result = QueryPlanBuilder.buildFromTree( request.topNode, statement );
        } catch ( Exception e ) {
            log.error( "Caught exception while building the plan builder tree", e );
            return RelationalResult.builder().error( e.getMessage() ).build();
        }

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = result.getTupleType();
        final List<Pair<Integer, String>> fields = Pair.zip( IntStream.range( 0, rowType.getFieldCount() ).boxed().toList(), rowType.getFieldNames() );
        final AlgCollation collation =
                result instanceof Sort
                        ? ((Sort) result).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( result, result.getTupleType(), Kind.SELECT, fields, collation );

        // Prepare
        PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( root, true );

        if ( request.createView ) {

            String viewName = request.viewName;
            boolean replace = false;
            String viewType;

            if ( request.freshness != null ) {
                viewType = "Materialized View";
                DataStore<?> store = AdapterManager.getInstance().getStore( request.store ).orElseThrow();
                List<DataStore<?>> stores = new ArrayList<>();
                stores.add( store );

                PlacementType placementType = PlacementType.MANUAL;

                List<String> columns = new ArrayList<>();
                root.alg.getTupleType().getFields().forEach( f -> columns.add( f.getName() ) );

                // Default Namespace
                long namespaceId = transaction.getDefaultNamespace().id;

                MaterializedCriteria materializedCriteria;
                if ( request.freshness.toUpperCase().equalsIgnoreCase( CriteriaType.INTERVAL.toString() ) ) {
                    materializedCriteria = new MaterializedCriteria( CriteriaType.INTERVAL, Integer.parseInt( request.interval ), getFreshnessType( request.timeUnit ) );
                } else if ( request.freshness.toUpperCase().equalsIgnoreCase( CriteriaType.UPDATE.toString() ) ) {
                    materializedCriteria = new MaterializedCriteria( CriteriaType.UPDATE, Integer.parseInt( request.interval ) );
                } else if ( request.freshness.toUpperCase().equalsIgnoreCase( CriteriaType.MANUAL.toString() ) ) {
                    materializedCriteria = new MaterializedCriteria( CriteriaType.MANUAL );
                } else {
                    materializedCriteria = new MaterializedCriteria();
                }

                Gson gson = new Gson();

                DdlManager.getInstance().createMaterializedView(
                        viewName,
                        namespaceId,
                        root,
                        replace,
                        statement,
                        stores,
                        placementType,
                        columns,
                        materializedCriteria,
                        gson.toJson( request.topNode ),
                        QueryLanguage.from( "rel" ),
                        false,
                        false
                );


            } else {

                viewType = "View";
                List<DataStore<?>> store = null;
                PlacementType placementType = PlacementType.AUTOMATIC;

                List<String> columns = new ArrayList<>();
                root.alg.getTupleType().getFields().forEach( f -> columns.add( f.getName() ) );

                // Default Namespace
                long namespaceId = transaction.getDefaultNamespace().id;

                Gson gson = new Gson();

                DdlManager.getInstance().createView(
                        viewName,
                        namespaceId,
                        root.alg,
                        root.collation,
                        replace,
                        statement,
                        placementType,
                        columns,
                        gson.toJson( request.topNode ),
                        QueryLanguage.from( "rel" )
                );


            }
            try {
                transaction.commit();
            } catch ( TransactionException e ) {
                log.error( "Caught exception while creating View from Planbuilder.", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException transactionException ) {
                    log.error( "Exception while rollback", transactionException );
                }
                throw new GenericRuntimeException( e );
            }

            return RelationalResult.builder().query( "Created " + viewType + " \"" + viewName + "\" from logical query plan" ).build();
        }

        List<List<PolyValue>> rows;
        try {
            ResultIterator iterator = polyImplementation.execute( statement, getPageSize() );
            rows = iterator.getNextBatch();
            iterator.close();
        } catch ( Exception e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            return RelationalResult.builder().error( e.getMessage() ).build();
        }

        UiColumnDefinition[] header = new UiColumnDefinition[polyImplementation.getTupleType().getFieldCount()];
        int counter = 0;
        for ( AlgDataTypeField col : polyImplementation.getTupleType().getFields() ) {
            header[counter++] = UiColumnDefinition.builder()
                    .name( col.getName() )
                    .dataType( col.getType().getFullTypeString() )
                    .nullable( col.getType().isNullable() )
                    .precision( col.getType().getPrecision() ).build();
        }

        List<String[]> data = LanguageCrud.computeResultData( rows, List.of( header ), statement.getTransaction() );

        try {
            executionTime += System.nanoTime() - temp;
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while committing the plan builder tree", e );
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
            throw new GenericRuntimeException( e );
        }
        RelationalResult finalResult = RelationalResult.builder()
                .header( header )
                .data( data.toArray( new String[0][] ) )
                .xid( transaction.getXid().toString() )
                .query( "Execute logical query plan" )
                .build();

        if ( queryAnalyzer != null ) {
            InformationPage p1 = new InformationPage( "Query analysis", "Analysis of the query." );
            InformationGroup g1 = new InformationGroup( p1, "Execution time" );
            InformationText text;
            if ( executionTime < 1e4 ) {
                text = new InformationText( g1, String.format( "Execution time: %d nanoseconds", executionTime ) );
            } else {
                long millis = TimeUnit.MILLISECONDS.convert( executionTime, TimeUnit.NANOSECONDS );
                // format time: see: https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java#answer-625444
                DateFormat df = new SimpleDateFormat( "m 'min' s 'sec' S 'ms'" );
                String durationText = df.format( new Date( millis ) );
                text = new InformationText( g1, String.format( "Execution time: %s", durationText ) );
            }
            queryAnalyzer.addPage( p1 );
            queryAnalyzer.addGroup( g1 );
            queryAnalyzer.registerInformation( text );
        }

        return finalResult;
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
        ctx.json( PolyType.allowedFieldTypes().stream().map( PolyTypeModel::from ).collect( Collectors.toList() ) );
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
        String range = ctx.req.getHeader( "Range" );
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
                ctx.res.setHeader( "Accept-Ranges", "bytes" );
                ctx.status( 206 );//partial content
                int len = Long.valueOf( rangeEnd - rangeStart ).intValue() + 1;
                ctx.res.setHeader( "Content-Range", String.format( "bytes %d-%d/%d", rangeStart, rangeEnd, fileLength ) );

                RandomAccessFile raf = new RandomAccessFile( f, "r" );
                raf.seek( rangeStart );
                ServletOutputStream os = ctx.res.getOutputStream();
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
                ctx.res.setContentLengthLong( (int) fileLength );
                try ( FileInputStream fis = new FileInputStream( f ); ServletOutputStream os = ctx.res.getOutputStream() ) {
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
        ctx.res.setContentLengthLong( zipFile.length() );
        try ( OutputStream os = ctx.res.getOutputStream(); InputStream is = new FileInputStream( zipFile ) ) {
            IOUtils.copy( is, os );
        } catch ( IOException e ) {
            log.error( "Could not write zipOutputStream to response", e );
            ctx.status( 500 );
        }
        zipFile.delete();
        ctx.result( "" );
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
        return getTransaction( false, true, this );
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, TransactionManager transactionManager, long userId, long databaseId ) {
        return getTransaction( analyze, useCache, transactionManager, userId, databaseId, ORIGIN );
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, TransactionManager transactionManager, long userId, long namespaceId, String origin ) {
        Transaction transaction = transactionManager.startTransaction(
                userId,
                namespaceId,
                analyze,
                origin,
                MultimediaFlavor.FILE );
        transaction.setUseCache( useCache );
        return transaction;
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, Crud crud ) {
        return getTransaction( analyze, useCache, crud.transactionManager, Catalog.defaultUserId, Catalog.defaultNamespaceId );
    }


    void addDockerInstance( final Context ctx ) {
        Map<String, Object> config = gson.fromJson( ctx.body(), Map.class );
        DockerSetupResult res = DockerSetupHelper.newDockerInstance(
                (String) config.getOrDefault( "host", "" ),
                (String) config.getOrDefault( "alias", "" ),
                (String) config.getOrDefault( "registry", "" ),
                ((Double) config.getOrDefault( "communicationPort", (double) ConfigDocker.COMMUNICATION_PORT )).intValue(),
                ((Double) config.getOrDefault( "handshakePort", (double) ConfigDocker.HANDSHAKE_PORT )).intValue(),
                ((Double) config.getOrDefault( "proxyPort", (double) ConfigDocker.PROXY_PORT )).intValue(),
                true
        );

        Map<String, Object> json = new HashMap<>( res.getMap() );
        json.put( "instances", DockerManager.getInstance().getDockerInstances().values().stream().map( DockerInstance::getMap ).collect( Collectors.toList() ) );
        ctx.json( json );
    }


    void testDockerInstance( final Context ctx ) {
        int dockerId = Integer.parseInt( ctx.pathParam( "dockerId" ) );

        Optional<DockerInstance> maybeDockerInstance = DockerManager.getInstance().getInstanceById( dockerId );
        if ( maybeDockerInstance.isPresent() ) {
            ctx.json( maybeDockerInstance.get().probeDockerStatus() );
        } else {
            ctx.json( Map.of(
                    "successful", false,
                    "errorMessage", "No instance with that id"
            ) );
            ctx.status( 404 );
        }
    }


    void getDockerInstances( final Context ctx ) {
        ctx.json( DockerManager.getInstance().getDockerInstances().values().stream().map( DockerInstance::getMap ).collect( Collectors.toList() ) );
    }


    void getDockerInstance( final Context ctx ) {
        int dockerId = Integer.parseInt( ctx.pathParam( "dockerId" ) );

        Map<String, Object> res = DockerManager.getInstance().getInstanceById( dockerId ).map( DockerInstance::getMap ).orElse( Map.of() );

        ctx.json( res );
    }


    void updateDockerInstance( final Context ctx ) {
        Map<String, String> config = gson.fromJson( ctx.body(), Map.class );

        DockerUpdateResult res = DockerSetupHelper.updateDockerInstance( Integer.parseInt( config.getOrDefault( "id", "-1" ) ), config.getOrDefault( "hostname", "" ), config.getOrDefault( "alias", "" ), config.getOrDefault( "registry", "" ) );

        ctx.json( res.getMap() );
    }


    void reconnectToDockerInstance( final Context ctx ) {
        Map<String, String> config = gson.fromJson( ctx.body(), Map.class );

        DockerReconnectResult res = DockerSetupHelper.reconnectToInstance( Integer.parseInt( config.getOrDefault( "id", "-1" ) ) );

        ctx.json( res.getMap() );
    }


    void removeDockerInstance( final Context ctx ) {
        Map<String, String> config = gson.fromJson( ctx.body(), Map.class );
        int id = Integer.parseInt( config.getOrDefault( "id", "-1" ) );
        if ( id == -1 ) {
            throw new GenericRuntimeException( "Invalid id" );
        }

        String res = DockerSetupHelper.removeDockerInstance( id );

        ctx.json( Map.of(
                "error", res,
                "instances", DockerManager.getInstance().getDockerInstances().values().stream().map( DockerInstance::getMap ).collect( Collectors.toList() ),
                "status", AutoDocker.getInstance().getStatus()
        ) );
    }


    void getAutoDockerStatus( final Context ctx ) {
        ctx.json( AutoDocker.getInstance().getStatus() );
    }


    void doAutoHandshake( final Context ctx ) {
        boolean success = AutoDocker.getInstance().doAutoConnect();
        ctx.json( Map.of(
                "success", success,
                "status", AutoDocker.getInstance().getStatus(),
                "instances", DockerManager.getInstance().getDockerInstances().values().stream().map( DockerInstance::getMap ).collect( Collectors.toList() )
        ) );
    }


    void startHandshake( final Context ctx ) {
        String hostname = ctx.body();
        ctx.json( HandshakeManager.getInstance().restartOrGetHandshake( hostname ) );
    }


    void getHandshake( final Context ctx ) {
        String hostname = ctx.pathParam( "hostname" );
        Map<String, Object> dockerInstance = DockerManager.getInstance().getDockerInstances()
                .values()
                .stream()
                .filter( d -> d.getHost().hostname().equals( hostname ) )
                .map( DockerInstance::getMap )
                .findFirst()
                .orElse( Map.of() );
        ctx.json( Map.of(
                        "handshake", HandshakeManager.getInstance().getHandshake( hostname ),
                        "instance", dockerInstance
                )
        );
    }


    void cancelHandshake( final Context ctx ) {
        String hostname = ctx.body();
        if ( HandshakeManager.getInstance().cancelHandshake( hostname ) ) {
            ctx.status( 200 );
        } else {
            ctx.status( 404 );
        }
    }


    void getDockerSettings( final Context ctx ) {
        ctx.json( Map.of(
                "registry", RuntimeConfig.DOCKER_CONTAINER_REGISTRY.getString()
        ) );
    }


    void changeDockerSettings( final Context ctx ) {
        Map<String, String> config = gson.fromJson( ctx.body(), Map.class );
        String newRegistry = config.get( "registry" );
        if ( newRegistry != null ) {
            RuntimeConfig.DOCKER_CONTAINER_REGISTRY.setString( newRegistry );
        }
        getDockerSettings( ctx );
    }


    /**
     * Loads the plugin in the supplied path.
     */
    public void loadPlugins( final Context ctx ) {
        ctx.uploadedFiles( "plugins" ).forEach( file -> {
            String[] splits = file.getFilename().split( "/" );
            String normalizedFileName = splits[splits.length - 1];
            splits = normalizedFileName.split( "\\\\" );
            normalizedFileName = splits[splits.length - 1];
            File f = new File( System.getProperty( "user.home" ), ".polypheny/plugins/" + normalizedFileName );
            try {
                FileUtils.copyInputStreamToFile( file.getContent(), f );
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
