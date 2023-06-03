/*
 * Copyright 2019-2023 The Polypheny Project
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


import static org.polypheny.db.adapter.ConnectionMethod.LINK;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import io.javalin.http.Context;
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
import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.PolyImplementation;
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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalMaterializedView;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.NameGenerator;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceInformation;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceInformationRequest;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationStacktrace;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.partition.PartitionFunctionInfo;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.plugins.PolyPluginManager.PluginStatus;
import org.polypheny.db.processing.ExtendedQueryParameters;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyStream;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphObject;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.FileInputHandle;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.crud.StatisticCrud;
import org.polypheny.db.webui.models.AdapterModel;
import org.polypheny.db.webui.models.DbColumn;
import org.polypheny.db.webui.models.DbTable;
import org.polypheny.db.webui.models.ForeignKey;
import org.polypheny.db.webui.models.Index;
import org.polypheny.db.webui.models.MaterializedInfos;
import org.polypheny.db.webui.models.PartitionFunctionModel;
import org.polypheny.db.webui.models.PartitionFunctionModel.FieldType;
import org.polypheny.db.webui.models.PartitionFunctionModel.PartitionFunctionColumn;
import org.polypheny.db.webui.models.PathAccessRequest;
import org.polypheny.db.webui.models.Placement;
import org.polypheny.db.webui.models.Placement.RelationalStore;
import org.polypheny.db.webui.models.QueryInterfaceModel;
import org.polypheny.db.webui.models.Schema;
import org.polypheny.db.webui.models.SidebarElement;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.TableConstraint;
import org.polypheny.db.webui.models.Uml;
import org.polypheny.db.webui.models.UnderlyingTables;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest.Update;
import org.polypheny.db.webui.models.requests.ColumnRequest;
import org.polypheny.db.webui.models.requests.ConstraintRequest;
import org.polypheny.db.webui.models.requests.EditTableRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest.ModifyPartitionRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.RelAlgRequest;
import org.polypheny.db.webui.models.requests.SchemaTreeRequest;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.webui.models.results.Result.ResultBuilder;
import org.polypheny.db.webui.models.results.ResultType;


@Slf4j
public class Crud implements InformationObserver {

    private static final Gson gson = new Gson();
    @Getter
    private final TransactionManager transactionManager;
    @Getter
    private final long databaseId;
    @Getter
    private final long userId;

    public final LanguageCrud languageCrud;
    public final StatisticCrud statisticCrud;
    private final Catalog catalog = Catalog.getInstance();


    /**
     * Constructor
     *
     * @param transactionManager The Polypheny-DB transaction manager
     */
    Crud( final TransactionManager transactionManager, final long userId, final long databaseId ) {
        this.transactionManager = transactionManager;
        this.databaseId = databaseId;
        this.userId = userId;
        this.languageCrud = new LanguageCrud( this );
        this.statisticCrud = new StatisticCrud( this );
    }


    /**
     * Closes analyzers and deletes temporary files.
     */
    public static void cleanupOldSession( ConcurrentHashMap<String, Set<String>> sessionXIds, final String sessionId ) {
        Set<String> xIds = sessionXIds.remove( sessionId );
        if ( xIds == null || xIds.size() == 0 ) {
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
    Result getTable( final UIRequest request ) {
        Transaction transaction = getTransaction();
        ResultBuilder<?, ?> resultBuilder;

        StringBuilder query = new StringBuilder();
        String where = "";
        if ( request.filter != null ) {
            where = filterTable( request.filter );
        }
        String orderBy = "";
        if ( request.sortState != null ) {
            orderBy = sortTable( request.sortState );
        }
        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        query.append( "SELECT * FROM " )
                .append( tableId )
                .append( where )
                .append( orderBy );
        if ( !request.noLimit ) {
            query.append( " LIMIT " )
                    .append( getPageSize() )
                    .append( " OFFSET " )
                    .append( (Math.max( 0, request.currentPage - 1 )) * getPageSize() );
        }

        try {
            resultBuilder = executeSqlSelect( transaction.createStatement(), request, query.toString(), request.noLimit, this );
            resultBuilder.xid( transaction.getXid().toString() );
        } catch ( Exception e ) {
            if ( request.filter != null ) {
                resultBuilder = Result.builder().error( "Error while filtering table " + request.tableId );
            } else {
                resultBuilder = Result.builder().error( "Could not fetch table " + request.tableId );
                log.error( "Caught exception while fetching a table", e );
            }
            try {
                transaction.rollback();
                return resultBuilder.build();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        // determine if it is a view or a table
        LogicalTable catalogTable = catalog.getSnapshot().rel().getTables( t[0], t[1] ).orElseThrow();
        resultBuilder.namespaceType( catalogTable.namespaceType );
        if ( catalogTable.modifiable ) {
            resultBuilder.type( ResultType.TABLE );
        } else {
            resultBuilder.type( ResultType.VIEW );
        }

        //get headers with default values
        ArrayList<DbColumn> cols = new ArrayList<>();
        ArrayList<String> primaryColumns;
        if ( catalogTable.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
            primaryColumns = new ArrayList<>( primaryKey.getColumnNames() );
        } else {
            primaryColumns = new ArrayList<>();
        }
        for ( LogicalColumn logicalColumn : catalog.getSnapshot().rel().getColumns( catalogTable.id ) ) {
            String defaultValue = logicalColumn.defaultValue == null ? null : logicalColumn.defaultValue.value;
            String collectionsType = logicalColumn.collectionsType == null ? "" : logicalColumn.collectionsType.getName();
            cols.add(
                    DbColumn.builder()
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
                            .sort( request.sortState == null ? new SortState() : request.sortState.get( logicalColumn.name ) )
                            .filter( request.filter == null || request.filter.get( logicalColumn.name ) == null ? "" : request.filter.get( logicalColumn.name ) ).build() );
        }
        resultBuilder.header( cols.toArray( new DbColumn[0] ) );

        resultBuilder.currentPage( request.currentPage ).table( request.tableId );
        int tableSize = 0;
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


    void getSchemaTree( final Context ctx ) {
        SchemaTreeRequest request = ctx.bodyAsClass( SchemaTreeRequest.class );
        ArrayList<SidebarElement> result = new ArrayList<>();

        if ( request.depth < 1 ) {
            log.error( "Trying to fetch a schemaTree with depth < 1" );
            ctx.json( new ArrayList<>() );
        }

        List<LogicalNamespace> schemas = catalog.getSnapshot().getNamespaces( null );
        // remove unwanted namespaces
        schemas = schemas.stream().filter( s -> request.dataModels.contains( s.namespaceType ) ).collect( Collectors.toList() );
        for ( LogicalNamespace schema : schemas ) {
            SidebarElement schemaTree = new SidebarElement( schema.name, schema.name, schema.namespaceType, "", getIconName( schema.namespaceType ) );

            if ( request.depth > 1 && schema.namespaceType != NamespaceType.GRAPH ) {
                ArrayList<SidebarElement> collectionTree = new ArrayList<>();
                List<LogicalTable> tables = catalog.getSnapshot().rel().getTables( schema.id, null );
                for ( LogicalTable table : tables ) {
                    String icon = "fa fa-table";
                    if ( table.entityType == EntityType.SOURCE ) {
                        icon = "fa fa-plug";
                    } else if ( table.entityType == EntityType.VIEW ) {
                        icon = "icon-eye";
                    }
                    if ( table.entityType != EntityType.VIEW && schema.namespaceType == NamespaceType.DOCUMENT ) {
                        icon = "cui-description";
                    }

                    SidebarElement tableElement = new SidebarElement( schema.name + "." + table.name, table.name, schema.namespaceType, request.routerLinkRoot, icon );
                    if ( request.depth > 2 ) {
                        List<LogicalColumn> columns = catalog.getSnapshot().rel().getColumns( table.id );
                        for ( LogicalColumn column : columns ) {
                            tableElement.addChild( new SidebarElement( schema.name + "." + table.name + "." + column.name, column.name, schema.namespaceType, request.routerLinkRoot, icon ).setCssClass( "sidebarColumn" ) );
                        }
                    }

                    if ( request.views ) {
                        if ( table.entityType == EntityType.ENTITY || table.entityType == EntityType.SOURCE ) {
                            tableElement.setTableType( "TABLE" );
                        } else if ( table.entityType == EntityType.VIEW ) {
                            tableElement.setTableType( "VIEW" );
                        } else if ( table.entityType == EntityType.MATERIALIZED_VIEW ) {
                            tableElement.setTableType( "MATERIALIZED" );
                        }
                    }

                    collectionTree.add( tableElement );
                }

                if ( request.showTable ) {
                    schemaTree.addChild( new SidebarElement( schema.name + ".tables", "tables", schema.namespaceType, request.routerLinkRoot, "fa fa-table" ).addChildren( collectionTree ).setRouterLink( "" ) );
                } else {
                    schemaTree.addChildren( collectionTree ).setRouterLink( "" );
                }
            }
            if ( schema.namespaceType == NamespaceType.GRAPH ) {
                schemaTree.setRouterLink( request.routerLinkRoot + "/" + schema.name );
            }

            result.add( schemaTree );
        }

        ctx.json( result );
    }


    private String getIconName( NamespaceType namespaceType ) {
        switch ( namespaceType ) {
            case RELATIONAL:
                return "cui-layers";
            case DOCUMENT:
                return "cui-folder";
            case GRAPH:
                return "cui-graph";
        }
        throw new UnsupportedOperationException( "Namespace type is not supported." );
    }


    /**
     * Get all tables of a schema
     */
    void getTables( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        String namespaceName = request.schema != null ? request.schema : Catalog.defaultNamespaceName;
        long namespaceId = catalog.getSnapshot().getNamespace( namespaceName ).id;

        List<LogicalTable> tables = catalog.getSnapshot().rel().getTables( namespaceId, null );
        ArrayList<DbTable> result = new ArrayList<>();
        for ( LogicalTable t : tables ) {
            result.add( new DbTable( t.name, namespaceName, t.modifiable, t.entityType ) );
        }
        ctx.json( result );
    }


    void renameTable( final Context ctx ) {
        Index table = ctx.bodyAsClass( Index.class );
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" RENAME TO \"%s\"", table.getSchema(), table.getTable(), table.getName() );
        Transaction transaction = getTransaction();
        Result result;
        try {
            int rows = executeSqlUpdate( transaction, query );
            result = Result.builder().affectedRows( rows ).generatedQuery( query ).build();
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Drop or truncate a table
     */
    void dropTruncateTable( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        StringBuilder query = new StringBuilder();
        if ( request.tableType != null && request.action.equalsIgnoreCase( "drop" ) && request.tableType.equals( "VIEW" ) ) {
            query.append( "DROP VIEW " );
        } else if ( request.action.equalsIgnoreCase( "drop" ) ) {
            query.append( "DROP TABLE " );
        } else if ( request.action.equalsIgnoreCase( "truncate" ) ) {
            query.append( "TRUNCATE TABLE " );
        }
        String tableId = String.format( "\"%s\".\"%s\"", request.schema, request.table );
        query.append( tableId );
        try {
            int a = executeSqlUpdate( transaction, query.toString() );
            result = Result.builder().affectedRows( a ).generatedQuery( query.toString() ).build();
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping or truncating a table", e );
            result = Result.builder().error( e.getMessage() ).generatedQuery( query.toString() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Create a new table
     */
    void createTable( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        Transaction transaction = getTransaction();
        StringBuilder query = new StringBuilder();
        StringJoiner colJoiner = new StringJoiner( "," );
        String tableId = String.format( "\"%s\".\"%s\"", request.schema, request.table );
        query.append( "CREATE TABLE " ).append( tableId ).append( "(" );
        StringBuilder colBuilder;
        Result result;
        StringJoiner primaryKeys = new StringJoiner( ",", "PRIMARY KEY (", ")" );
        int primaryCounter = 0;
        for ( DbColumn col : request.columns ) {
            colBuilder = new StringBuilder();
            colBuilder.append( "\"" ).append( col.name ).append( "\" " ).append( col.dataType );
            if ( col.precision != null ) {
                colBuilder.append( "(" ).append( col.precision );
                if ( col.scale != null ) {
                    colBuilder.append( "," ).append( col.scale );
                }
                colBuilder.append( ")" );
            }
            if ( col.collectionsType != null && !col.collectionsType.equals( "" ) ) {
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
        if ( request.store != null && !request.store.equals( "" ) ) {
            query.append( String.format( " ON STORE \"%s\"", request.store ) );
        }

        try {
            int a = executeSqlUpdate( transaction, query.toString() );
            result = Result.builder().affectedRows( a ).generatedQuery( query.toString() ).build();
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating a table", e );
            result = Result.builder().error( e.getMessage() ).generatedQuery( query.toString() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback CREATE TABLE statement: {}", ex.getMessage(), ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Initialize a multipart request, so that the values can be fetched with request.raw().getPart( name )
     *
     * @param ctx Spark request
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
    void insertRow( final Context ctx ) {
        ctx.contentType( "multipart/form-data" );
        initMultipart( ctx );
        String tableId = null;
        try {
            tableId = new BufferedReader( new InputStreamReader( ctx.req.getPart( "tableId" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
        } catch ( IOException | ServletException e ) {
            ctx.json( Result.builder().error( e.getMessage() ).build() );
        }
        String[] split = tableId.split( "\\." );
        tableId = String.format( "\"%s\".\"%s\"", split[0], split[1] );

        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StringJoiner columns = new StringJoiner( ",", "(", ")" );
        StringJoiner values = new StringJoiner( ",", "(", ")" );

        LogicalTable table = catalog.getSnapshot().rel().getTable( split[0], split[1] ).orElseThrow();
        List<LogicalColumn> logicalColumns = catalog.getSnapshot().rel().getColumns( table.id );
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
                            if ( value.length() == 0 ) {
                                value = BsonUtil.getObjectId();
                            }
                        }
                        values.add( uiValueToSql( value, logicalColumn.type, logicalColumn.collectionsType ) );
                    } else {
                        values.add( "?" );
                        FileInputHandle fih = new FileInputHandle( statement, part.getInputStream() );
                        statement.getDataContext().addParameterValues( i++, logicalColumn.getAlgDataType( transaction.getTypeFactory() ), ImmutableList.of( PolyStream.of( fih.getData() ) ) );
                    }
                }
            }
        } catch ( IOException | ServletException e ) {
            log.error( "Could not generate INSERT statement", e );
            ctx.status( 500 );
            ctx.json( Result.builder().error( e.getMessage() ).build() );
        }

        String query = String.format( "INSERT INTO %s %s VALUES %s", tableId, columns, values );
        try {
            int numRows = executeSqlUpdate( statement, transaction, query );
            transaction.commit();
            ctx.json( Result.builder().affectedRows( numRows ).generatedQuery( query ).build() );
        } catch ( Exception | TransactionException e ) {
            log.info( "Generated query: {}", query );
            log.error( "Could not insert row", e );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught error while rolling back transaction", e2 );
            }
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( query ).build() );
        }
    }


    /**
     * Run any query coming from the SQL console
     */
    public static List<Result> anySqlQuery( final QueryRequest request, final Session session, Crud crud ) {
        Transaction transaction = getTransaction( request.analyze, request.cache, crud );

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        ArrayList<Result> results = new ArrayList<>();
        boolean autoCommit = true;

        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( request.analyze ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( crud );
        }

        // TODO: make it possible to use pagination

        // No autoCommit if the query has commits.
        // Ignore case: from: https://alvinalexander.com/blog/post/java/java-how-case-insensitive-search-string-matches-method
        Pattern p = Pattern.compile( ".*(COMMIT|ROLLBACK).*", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
        Matcher m = p.matcher( request.query );
        if ( m.matches() ) {
            autoCommit = false;
        }

        long executionTime = 0;
        long temp = 0;
        // remove all comments
        String allQueries = request.query;
        //remove comments
        allQueries = allQueries.replaceAll( "(?s)(\\/\\*.*?\\*\\/)", "" );
        allQueries = allQueries.replaceAll( "(?m)(--.*?$)", "" );
        //remove whitespace at the end
        allQueries = allQueries.replaceAll( "(\\s*)$", "" );
        String[] queries = allQueries.split( ";(?=(?:[^\']*\'[^\']*\')*[^\']*$)" );
        boolean noLimit;
        for ( String query : queries ) {
            Result result;
            if ( !transaction.isActive() ) {
                transaction = getTransaction( request.analyze, request.cache, crud );
            }
            if ( Pattern.matches( "(?si:[\\s]*COMMIT.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    transaction.commit();
                    executionTime += System.nanoTime() - temp;
                    transaction = getTransaction( request.analyze, request.cache, crud );
                    results.add( Result.builder().generatedQuery( query ).build() );
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
                    results.add( Result.builder().generatedQuery( query ).build() );
                } catch ( TransactionException e ) {
                    log.error( "Caught exception while rolling back a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                }
            } else if ( Pattern.matches( "(?si:^[\\s]*[/(\\s]*SELECT.*)", query ) ) {
                // Add limit if not specified
                Pattern p2 = Pattern.compile( ".*?(?si:limit)[\\s\\S]*", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
                if ( !p2.matcher( query ).find() && !request.noLimit ) {
                    noLimit = false;
                }
                //If the user specifies a limit
                else {
                    noLimit = true;
                }
                // decrease limit if it is too large
                /*else {
                    Pattern pattern = Pattern.compile( "(.*?LIMIT[\\s+])(\\d+)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
                    Matcher limitMatcher = pattern.matcher( query );
                    if ( limitMatcher.find() ) {
                        int limit = Integer.parseInt( limitMatcher.group( 2 ) );
                        if ( limit > getPageSize() ) {
                            // see https://stackoverflow.com/questions/38296673/replace-group-1-of-java-regex-with-out-replacing-the-entire-regex?rq=1
                            query = limitMatcher.replaceFirst( "$1 " + getPageSize() );
                        }
                    }
                }*/
                try {
                    temp = System.nanoTime();
                    result = executeSqlSelect( transaction.createStatement(), request, query, noLimit, crud )
                            .generatedQuery( query )
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
                        result = Result.builder().error( ((AvaticaRuntimeException) e.getCause()).getErrorMessage() ).build();
                    } else {
                        result = Result.builder().error( e.getCause().getMessage() ).build();
                    }
                    results.add( result.toBuilder().generatedQuery( query ).xid( transaction.getXid().toString() ).build() );
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

                    results.add( Result.builder().affectedRows( numOfRows ).generatedQuery( query ).xid( transaction.getXid().toString() ).build() );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze, request.cache, crud );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    results.add( Result.builder().error( e.getMessage() ).generatedQuery( query ).xid( transaction.getXid().toString() ).build() );
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
            results.add( Result.builder().error( e.getMessage() ).build() );
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
    }


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
            //noinspection SuspiciousDateFormat
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
    private String computeWherePK( final String namespace, final String table, final Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", "", "" );
        Map<String, LogicalColumn> catalogColumns = getCatalogColumns( namespace, table );
        if ( catalogColumns.isEmpty() ) {
            throw new RuntimeException();
        }

        LogicalTable catalogTable = catalog.getSnapshot().rel().getTable( namespace, table ).orElseThrow();
        LogicalPrimaryKey pk = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
        for ( long colId : pk.columnIds ) {
            String colName = catalog.getSnapshot().rel().getColumn( colId ).orElseThrow().name;
            String condition;
            if ( filter.containsKey( colName ) ) {
                String val = filter.get( colName );
                LogicalColumn col = catalogColumns.get( colName );
                condition = uiValueToSql( val, col.type, col.collectionsType );
                condition = String.format( "\"%s\" = %s", colName, condition );
                joiner.add( condition );
            }
        }
        return " WHERE " + joiner;
    }


    /**
     * Delete a row from a table. The row is determined by the value of every PK column in that row (conjunction).
     */
    void deleteRow( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        StringBuilder builder = new StringBuilder();
        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        builder.append( "DELETE FROM " ).append( tableId ).append( computeWherePK( t[0], t[1], request.data ) );
        try {
            int numOfRows = executeSqlUpdate( transaction, builder.toString() );
            if ( statisticCrud.isActiveTracking() ) {
                transaction.addChangedTable( tableId );
            }

            transaction.commit();
            result = Result.builder().affectedRows( numOfRows ).build();
        } catch ( TransactionException | Exception e ) {
            log.error( "Caught exception while deleting a row", e );
            result = Result.builder().error( e.getMessage() ).generatedQuery( builder.toString() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Update a row from a table. The row is determined by the value of every PK column in that row (conjunction).
     */
    void updateRow( final Context ctx ) throws ServletException, IOException {
        ctx.contentType( "multipart/form-data" );
        initMultipart( ctx );
        String tableId = null;
        Map<String, String> oldValues = null;
        try {
            tableId = new BufferedReader( new InputStreamReader( ctx.req.getPart( "tableId" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
            String _oldValues = new BufferedReader( new InputStreamReader( ctx.req.getPart( "oldValues" ).getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
            oldValues = gson.fromJson( _oldValues, Map.class );
        } catch ( IOException | ServletException e ) {
            ctx.json( Result.builder().error( e.getMessage() ).build() );
        }

        String[] split = tableId.split( "\\." );
        tableId = String.format( "\"%s\".\"%s\"", split[0], split[1] );

        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StringJoiner setStatements = new StringJoiner( ",", "", "" );

        LogicalNamespace namespace = catalog.getSnapshot().getNamespace( split[0] );

        List<LogicalColumn> logicalColumns = catalog.getSnapshot().rel().getColumns( new org.polypheny.db.catalog.logistic.Pattern( split[1] ), null );

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
                statement.getDataContext().addParameterValues( i++, null, ImmutableList.of( PolyStream.of( fih.getData() ) ) );
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append( "UPDATE " ).append( tableId ).append( " SET " ).append( setStatements.toString() ).append( computeWherePK( split[0], split[1], oldValues ) );

        Result result;
        try {
            int numOfRows = executeSqlUpdate( statement, transaction, builder.toString() );

            if ( numOfRows == 1 ) {
                if ( statisticCrud.isActiveTracking() ) {
                    transaction.addChangedTable( tableId );
                }
                transaction.commit();
                result = Result.builder().affectedRows( numOfRows ).build();
            } else {
                transaction.rollback();
                result = Result.builder().error( "Attempt to update " + numOfRows + " rows was blocked." ).generatedQuery( builder.toString() ).build();
            }
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a row", e );
            result = Result.builder().error( e.getMessage() ).generatedQuery( builder.toString() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
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
        int totalRows = 0;
        try {
            for ( Update update : request.updates ) {
                statement = transaction.createStatement();
                String query = update.getQuery( request.tableId, statement, ctx.req );
                totalRows += executeSqlUpdate( statement, transaction, query );
            }
            transaction.commit();
            ctx.json( Result.builder().affectedRows( totalRows ).build() );
        } catch ( ServletException | IOException | QueryExecutionException | TransactionException e ) {
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
                ctx.status( 500 ).json( Result.builder().error( e.getMessage() ).build() );
            }
            log.error( "The batch update failed", e );
            ctx.status( 500 ).json( Result.builder().error( e.getMessage() ).build() );
        }
    }


    /**
     * Get the columns of a table
     */
    void getColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        String[] t = request.tableId.split( "\\." );
        ArrayList<DbColumn> cols = new ArrayList<>();

        LogicalTable catalogTable = catalog.getSnapshot().rel().getTable( t[0], t[1] ).orElseThrow();
        ArrayList<String> primaryColumns;
        if ( catalogTable.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
            primaryColumns = new ArrayList<>( primaryKey.getColumnNames() );
        } else {
            primaryColumns = new ArrayList<>();
        }
        for ( LogicalColumn logicalColumn : catalog.getSnapshot().rel().getColumns( catalogTable.id ) ) {
            String defaultValue = logicalColumn.defaultValue == null ? null : logicalColumn.defaultValue.value;
            String collectionsType = logicalColumn.collectionsType == null ? "" : logicalColumn.collectionsType.getName();
            cols.add(
                    DbColumn.builder()
                            .name( logicalColumn.name )
                            .dataType( logicalColumn.type.getName() )
                            .collectionsType( collectionsType )
                            .nullable( logicalColumn.nullable )
                            .precision( logicalColumn.length )
                            .scale( logicalColumn.scale )
                            .dimension( logicalColumn.dimension )
                            .cardinality( logicalColumn.cardinality )
                            .primary( primaryColumns.contains( logicalColumn.name ) )
                            .defaultValue( defaultValue ).build() );
        }
        ResultBuilder<?, ?> result = Result.builder().header( cols.toArray( new DbColumn[0] ) );
        if ( catalogTable.entityType == EntityType.ENTITY ) {
            result.type( ResultType.TABLE );
        } else if ( catalogTable.entityType == EntityType.MATERIALIZED_VIEW ) {
            result.type( ResultType.MATERIALIZED );
        } else {
            result.type( ResultType.VIEW );
        }

        ctx.json( result.build() );
    }


    void getDataSourceColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        LogicalTable catalogTable = catalog.getSnapshot().rel().getTable( request.getSchemaName(), request.getTableName() ).orElseThrow();

        if ( catalogTable.entityType == EntityType.VIEW ) {

            List<DbColumn> columns = new ArrayList<>();
            List<LogicalColumn> cols = catalog.getSnapshot().rel().getColumns( catalogTable.id );
            for ( LogicalColumn col : cols ) {
                columns.add( DbColumn.builder()
                        .name( col.name )
                        .dataType( col.type.getName() )
                        .collectionsType( col.collectionsType == null ? "" : col.collectionsType.getName() )
                        .nullable( col.nullable )
                        .precision( col.length )
                        .scale( col.scale )
                        .dimension( col.dimension )
                        .cardinality( col.cardinality )
                        .primary( false )
                        .defaultValue( col.defaultValue == null ? null : col.defaultValue.value ).physicalName( col.name ).build()
                );

            }
            ctx.json( Result.builder().header( columns.toArray( new DbColumn[0] ) ).type( ResultType.VIEW ).build() );
        } else {
            List<AllocationEntity> allocs = catalog.getSnapshot().alloc().getFromLogical( catalogTable.id );
            if ( catalog.getSnapshot().alloc().getFromLogical( catalogTable.id ).size() != 1 ) {
                throw new RuntimeException( "The table has an unexpected number of placements!" );
            }

            long adapterId = allocs.get( 0 ).adapterId;
            LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
            List<String> pkColumnNames = primaryKey.getColumnNames();
            List<DbColumn> columns = new ArrayList<>();
            for ( AllocationColumn ccp : catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerTable( adapterId, catalogTable.id ) ) {
                LogicalColumn col = catalog.getSnapshot().rel().getColumn( ccp.columnId ).orElseThrow();
                columns.add( DbColumn.builder().name(
                        col.name ).dataType(
                        col.type.getName() ).collectionsType(
                        col.collectionsType == null ? "" : col.collectionsType.getName() ).nullable(
                        col.nullable ).precision(
                        col.length ).scale(
                        col.scale ).dimension(
                        col.dimension ).cardinality(
                        col.cardinality ).primary(
                        pkColumnNames.contains( col.name ) ).defaultValue(
                        col.defaultValue == null ? null : col.defaultValue.value ).build() );
            }
            ctx.json( Result.builder().header( columns.toArray( new DbColumn[0] ) ).type( ResultType.TABLE ).build() );
        }
    }


    /**
     * Get additional columns of the DataSource that are not mapped to the table.
     */
    void getAvailableSourceColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        LogicalTable table = catalog.getSnapshot().rel().getTable( request.getSchemaName(), request.getTableName() ).orElseThrow();
        Map<Long, List<Long>> placements = catalog.getSnapshot().alloc().getColumnPlacementsByAdapter( table.id );
        Set<Long> adapterIds = placements.keySet();
        if ( adapterIds.size() > 1 ) {
            log.warn( String.format( "The number of sources of an entity should not be > 1 (%s.%s)", request.getSchemaName(), request.getTableName() ) );
        }
        List<Result> exportedColumns = new ArrayList<>();
        for ( Long adapterId : adapterIds ) {
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( adapterId );
            if ( adapter instanceof DataSource<?> ) {
                DataSource<?> dataSource = (DataSource<?>) adapter;
                for ( Entry<String, List<ExportedColumn>> entry : dataSource.getExportedColumns().entrySet() ) {
                    List<DbColumn> columnList = new ArrayList<>();
                    for ( ExportedColumn col : entry.getValue() ) {
                        DbColumn dbCol = DbColumn.builder()
                                .name( col.name )
                                .dataType( col.type.getName() )
                                .collectionsType( col.collectionsType == null ? "" : col.collectionsType.getName() )
                                .nullable( col.nullable )
                                .precision( col.length )
                                .scale( col.scale )
                                .dimension( col.dimension )
                                .cardinality( col.cardinality )
                                .primary( col.primary )
                                .physicalName( col.physicalColumnName ).build();
                        columnList.add( dbCol );
                    }
                    exportedColumns.add( Result.builder().header( columnList.toArray( new DbColumn[0] ) ).table( entry.getKey() ).build() );
                    columnList.clear();
                }
                ctx.json( exportedColumns.toArray( new Result[0] ) );
                return;
            }

        }

        ctx.json( Result.builder().error( "Could not retrieve exported source fields." ).build() );
    }


    void getMaterializedInfo( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );

        LogicalTable catalogTable = getLogicalTable( request.schema, request.table );

        if ( catalogTable.entityType == EntityType.MATERIALIZED_VIEW ) {
            LogicalMaterializedView logicalMaterializedView = (LogicalMaterializedView) catalogTable;

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
            throw new RuntimeException( "only possible with materialized views" );
        }
    }


    private LogicalTable getLogicalTable( String schema, String table ) {
        return catalog.getSnapshot().rel().getTable( schema, table ).orElseThrow();
    }


    void updateMaterialized( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        ArrayList<String> queries = new ArrayList<>();
        StringBuilder sBuilder = new StringBuilder();

        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        String query = String.format( "ALTER MATERIALIZED VIEW %s FRESHNESS MANUAL", tableId );
        queries.add( query );

        result = Result.builder().affectedRows( 1 ).generatedQuery( queries.toString() ).build();
        try {
            for ( String q : queries ) {
                sBuilder.append( q );
                executeSqlUpdate( transaction, q );
            }
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a column", e );
            result = Result.builder().error( e.getMessage() ).affectedRows( 0 ).generatedQuery( sBuilder.toString() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught exception during rollback", e2 );
                result = Result.builder().error( e2.getMessage() ).affectedRows( 0 ).generatedQuery( sBuilder.toString() ).build();
            }
        }

        ctx.json( result );
    }


    void updateColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );
        Transaction transaction = getTransaction();

        DbColumn oldColumn = request.oldColumn;
        DbColumn newColumn = request.newColumn;
        Result result;
        ArrayList<String> queries = new ArrayList<>();
        StringBuilder sBuilder = new StringBuilder();

        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

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
                    !oldColumn.collectionsType.equals( newColumn.collectionsType ) ||
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
                if ( !newColumn.collectionsType.equals( "" ) ) {
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
                                request.newColumn.defaultValue = request.newColumn.defaultValue.replace( ",", "." );
                                BigDecimal b = new BigDecimal( request.newColumn.defaultValue );
                                query = query + b.toString();
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

        result = Result.builder().affectedRows( 1 ).generatedQuery( queries.toString() ).build();
        try {
            for ( String query : queries ) {
                sBuilder.append( query );
                executeSqlUpdate( transaction, query );
            }
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a column", e );
            result = Result.builder().error( e.getMessage() ).affectedRows( 0 ).generatedQuery( sBuilder.toString() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught exception during rollback", e2 );
                result = Result.builder().error( e2.getMessage() ).affectedRows( 0 ).generatedQuery( sBuilder.toString() ).build();
            }
        }

        ctx.json( result );
    }


    /**
     * Add a column to an existing table
     */
    void addColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );
        Transaction transaction = getTransaction();

        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

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
            if ( !request.newColumn.collectionsType.equals( "" ) ) {
                query = query + " " + request.newColumn.collectionsType;
                int dimension = request.newColumn.dimension == null ? -1 : request.newColumn.dimension;
                int cardinality = request.newColumn.cardinality == null ? -1 : request.newColumn.cardinality;
                query = query + String.format( "(%d,%d)", dimension, cardinality );
            }
            if ( !request.newColumn.nullable ) {
                query = query + " NOT NULL";
            }
        }
        if ( request.newColumn.defaultValue != null && !request.newColumn.defaultValue.equals( "" ) ) {
            query = query + " DEFAULT ";
            if ( request.newColumn.collectionsType != null && !request.newColumn.collectionsType.equals( "" ) ) {
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
                        request.newColumn.defaultValue = request.newColumn.defaultValue.replace( ",", "." );
                        BigDecimal b = new BigDecimal( request.newColumn.defaultValue );
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
        Result result;
        try {
            int affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = Result.builder().affectedRows( affectedRows ).generatedQuery( query ).build();
        } catch ( TransactionException | QueryExecutionException e ) {
            log.error( "Caught exception while adding a column", e );
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Delete a column of a table
     */
    void dropColumn( final Context ctx ) {
        ColumnRequest request = ctx.bodyAsClass( ColumnRequest.class );
        Transaction transaction = getTransaction();

        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result result;
        String query = String.format( "ALTER TABLE %s DROP COLUMN \"%s\"", tableId, request.oldColumn.name );
        try {
            int affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = Result.builder().affectedRows( affectedRows ).build();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a column", e );
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Get artificially generated index/foreign key/constraint names for placeholders in the UI
     */
    void getGeneratedNames( final Context ctx ) {
        String[] data = new String[3];
        data[0] = NameGenerator.generateConstraintName();
        data[1] = NameGenerator.generateForeignKeyName();
        data[2] = NameGenerator.generateIndexName();
        ctx.json( Result.builder().header( new DbColumn[0] ).data( new String[][]{ data } ).build() );
    }


    /**
     * Get constraints of a table
     */
    void getConstraints( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        Result result;

        String[] t = request.tableId.split( "\\." );
        ArrayList<TableConstraint> resultList = new ArrayList<>();
        Map<String, ArrayList<String>> temp = new HashMap<>();

        LogicalTable catalogTable = getLogicalTable( t[0], t[1] );

        // get primary key
        if ( catalogTable.primaryKey != null ) {
            LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
            for ( String columnName : primaryKey.getColumnNames() ) {
                if ( !temp.containsKey( "" ) ) {
                    temp.put( "", new ArrayList<>() );
                }
                temp.get( "" ).add( columnName );
            }
            for ( Map.Entry<String, ArrayList<String>> entry : temp.entrySet() ) {
                resultList.add( new TableConstraint( entry.getKey(), "PRIMARY KEY", entry.getValue() ) );
            }
        }

        // get unique constraints.
        temp.clear();
        List<LogicalConstraint> constraints = catalog.getSnapshot().rel().getConstraints( catalogTable.id );
        for ( LogicalConstraint logicalConstraint : constraints ) {
            if ( logicalConstraint.type == ConstraintType.UNIQUE ) {
                temp.put( logicalConstraint.name, new ArrayList<>( logicalConstraint.key.getColumnNames() ) );
            }
        }
        for ( Map.Entry<String, ArrayList<String>> entry : temp.entrySet() ) {
            resultList.add( new TableConstraint( entry.getKey(), "UNIQUE", entry.getValue() ) );
        }

        // the foreign keys are listed separately

        DbColumn[] header = { DbColumn.builder().name( "Name" ).build(), DbColumn.builder().name( "Type" ).build(), DbColumn.builder().name( "Columns" ).build() };
        ArrayList<String[]> data = new ArrayList<>();
        resultList.forEach( c -> data.add( c.asRow() ) );

        result = Result.builder().header( header ).data( data.toArray( new String[0][2] ) ).build();

        ctx.json( result );
    }


    void dropConstraint( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );
        Transaction transaction = getTransaction();

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
        Result result;
        try {
            int rows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = Result.builder().affectedRows( rows ).build();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a constraint", e );
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Add a primary key to a table
     */
    void addPrimaryKey( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );
        Transaction transaction = getTransaction();

        String[] t = request.table.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result result;
        if ( request.constraint.columns.length < 1 ) {
            result = Result.builder().error( "Cannot add primary key if no columns are provided." ).build();
            ctx.json( result );
            return;
        }
        StringJoiner joiner = new StringJoiner( ",", "(", ")" );
        for ( String s : request.constraint.columns ) {
            joiner.add( "\"" + s + "\"" );
        }
        String query = "ALTER TABLE " + tableId + " ADD PRIMARY KEY " + joiner;
        try {
            int rows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = Result.builder().affectedRows( rows ).generatedQuery( query ).build();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while adding a primary key", e );
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        ctx.json( result );
    }


    /**
     * Add a primary key to a table
     */
    void addUniqueConstraint( final Context ctx ) {
        ConstraintRequest request = ctx.bodyAsClass( ConstraintRequest.class );
        Transaction transaction = getTransaction();

        String[] t = request.table.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result result;
        if ( request.constraint.columns.length > 0 ) {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( String s : request.constraint.columns ) {
                joiner.add( "\"" + s + "\"" );
            }
            String query = "ALTER TABLE " + tableId + " ADD CONSTRAINT \"" + request.constraint.name + "\" UNIQUE " + joiner.toString();
            try {
                int rows = executeSqlUpdate( transaction, query );
                transaction.commit();
                result = Result.builder().affectedRows( rows ).generatedQuery( query ).build();
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while adding a unique constraint", e );
                result = Result.builder().error( e.getMessage() ).build();
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
            }
        } else {
            result = Result.builder().error( "Cannot add unique constraint if no columns are provided." ).build();
        }
        ctx.json( result );
    }


    /**
     * Get indexes of a table
     */
    void getIndexes( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        Result result;

        LogicalTable catalogTable = getLogicalTable( request.schema, request.table );
        List<LogicalIndex> logicalIndices = catalog.getSnapshot().rel().getIndexes( catalogTable.id, false );

        DbColumn[] header = {
                DbColumn.builder().name( "Name" ).build(),
                DbColumn.builder().name( "Columns" ).build(),
                DbColumn.builder().name( "Location" ).build(),
                DbColumn.builder().name( "Method" ).build(),
                DbColumn.builder().name( "Type" ).build() };

        ArrayList<String[]> data = new ArrayList<>();

        // Get explicit indexes
        for ( LogicalIndex logicalIndex : logicalIndices ) {
            String[] arr = new String[5];
            String storeUniqueName;
            if ( logicalIndex.location == 0 ) {
                // a polystore index
                storeUniqueName = "Polypheny-DB";
            } else {
                storeUniqueName = catalog.getSnapshot().getAdapter( logicalIndex.location ).uniqueName;
            }
            arr[0] = logicalIndex.name;
            arr[1] = String.join( ", ", logicalIndex.key.getColumnNames() );
            arr[2] = storeUniqueName;
            arr[3] = logicalIndex.methodDisplayName;
            arr[4] = logicalIndex.type.name();
            data.add( arr );
        }

        // Get functional indexes
        List<AllocationEntity> allocs = catalog.getSnapshot().alloc().getFromLogical( catalogTable.id );
        for ( AllocationEntity alloc : allocs ) {
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( alloc.adapterId );
            DataStore<?> store;
            if ( adapter instanceof DataStore<?> ) {
                store = (DataStore<?>) adapter;
            } else {
                break;
            }
            for ( FunctionalIndexInfo fif : store.getFunctionalIndexes( catalogTable ) ) {
                String[] arr = new String[5];
                arr[0] = "";
                arr[1] = String.join( ", ", fif.getColumnNames() );
                arr[2] = store.getUniqueName();
                arr[3] = fif.methodDisplayName;
                arr[4] = "FUNCTIONAL";
                data.add( arr );
            }
        }

        result = Result.builder().header( header ).data( data.toArray( new String[0][2] ) ).build();

        ctx.json( result );
    }


    /**
     * Drop an index of a table
     *
     * @param ctx
     */
    void dropIndex( final Context ctx ) {
        Index index = ctx.bodyAsClass( Index.class );
        Transaction transaction = getTransaction();

        String tableId = String.format( "\"%s\".\"%s\"", index.getSchema(), index.getTable() );
        String query = String.format( "ALTER TABLE %s DROP INDEX \"%s\"", tableId, index.getName() );
        Result result;
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = Result.builder().affectedRows( a ).generatedQuery( query ).build();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping an index", e );
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    /**
     * Create an index for a table
     */
    void createIndex( final Context ctx ) {
        Index index = ctx.bodyAsClass( Index.class );
        Transaction transaction = getTransaction();

        String tableId = String.format( "\"%s\".\"%s\"", index.getSchema(), index.getTable() );
        Result result;
        StringJoiner colJoiner = new StringJoiner( ",", "(", ")" );
        for ( String col : index.getColumns() ) {
            colJoiner.add( "\"" + col + "\"" );
        }
        String store = "POLYPHENY";
        if ( !index.getStoreUniqueName().equals( "Polypheny-DB" ) ) {
            store = index.getStoreUniqueName();
        }
        String onStore = String.format( "ON STORE \"%s\"", store );

        String query = String.format( "ALTER TABLE %s ADD INDEX \"%s\" ON %s USING \"%s\" %s", tableId, index.getName(), colJoiner, index.getMethod(), onStore );
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = Result.builder().affectedRows( a ).build();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating an index", e );
            result = Result.builder().error( e.getMessage() ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    void getUnderlyingTable( final Context ctx ) {

        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        LogicalTable catalogTable = getLogicalTable( request.getSchemaName(), request.getTableName() );

        if ( catalogTable.entityType == EntityType.VIEW ) {
            ImmutableMap<Long, List<Long>> underlyingTableOriginal = catalogTable.unwrap( LogicalView.class ).underlyingTables;
            Map<String, List<String>> underlyingTable = new HashMap<>();
            for ( Entry<Long, List<Long>> entry : underlyingTableOriginal.entrySet() ) {
                List<String> columns = new ArrayList<>();
                for ( Long ids : entry.getValue() ) {
                    columns.add( catalog.getSnapshot().rel().getColumn( ids ).orElseThrow().name );
                }
                underlyingTable.put( catalog.getSnapshot().rel().getTable( entry.getKey() ).orElseThrow().name, columns );
            }
            ctx.json( new UnderlyingTables( underlyingTable ) );
        } else {
            throw new RuntimeException( "Only possible with Views" );
        }
    }


    /**
     * Get placements of a table
     */
    void getPlacements( final Context ctx ) {
        Index index = ctx.bodyAsClass( Index.class );
        ctx.json( getPlacements( index ) );
    }


    private Placement getPlacements( final Index index ) {
        String schemaName = index.getSchema();
        String tableName = index.getTable();
        Snapshot snapshot = Catalog.getInstance().getSnapshot();

        LogicalTable table = getLogicalTable( schemaName, tableName );
        Placement p = new Placement( snapshot.alloc().getFromLogical( table.id ).size() > 1, snapshot.alloc().getPartitionGroupNames( table.id ), table.entityType );
        if ( table.entityType != EntityType.VIEW ) {
            long pkid = table.primaryKey;
            List<Long> pkColumnIds = snapshot.rel().getPrimaryKey( pkid ).orElseThrow().columnIds;
            LogicalColumn pkColumn = snapshot.rel().getColumn( pkColumnIds.get( 0 ) ).orElseThrow();
            List<AllocationColumn> pkPlacements = snapshot.alloc().getColumnFromLogical( pkColumn.id ).orElseThrow();
            for ( AllocationColumn placement : pkPlacements ) {
                Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId );
                PartitionProperty property = snapshot.alloc().getPartitionProperty( table.id );
                p.addAdapter( new RelationalStore(
                        adapter.getUniqueName(),
                        adapter.getUniqueName(),
                        snapshot.alloc().getColumnPlacementsOnAdapterPerTable( adapter.getAdapterId(), table.id ),
                        snapshot.alloc().getPartitionGroupsIndexOnDataPlacement( placement.adapterId, placement.tableId ),
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
        Index index = ctx.bodyAsClass( Index.class );
        if ( !index.getMethod().equalsIgnoreCase( "ADD" ) && !index.getMethod().equalsIgnoreCase( "DROP" ) && !index.getMethod().equalsIgnoreCase( "MODIFY" ) ) {
            ctx.json( Result.builder().error( "Invalid request" ).build() );
            return;
        }
        StringJoiner columnJoiner = new StringJoiner( ",", "(", ")" );
        int counter = 0;
        if ( !index.getMethod().equalsIgnoreCase( "DROP" ) ) {
            for ( String col : index.getColumns() ) {
                columnJoiner.add( "\"" + col + "\"" );
                counter++;
            }
        }
        String columnListStr = counter > 0 ? columnJoiner.toString() : "";
        String query = String.format(
                "ALTER TABLE \"%s\".\"%s\" %s PLACEMENT %s ON STORE \"%s\"",
                index.getSchema(),
                index.getTable(),
                index.getMethod().toUpperCase(),
                columnListStr,
                index.getStoreUniqueName() );
        Transaction transaction = getTransaction();
        int affectedRows = 0;
        try {
            affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( Result.builder().error( e.getMessage() ).build() );
        }
        ctx.json( Result.builder().affectedRows( affectedRows ).generatedQuery( query ).build() );
    }


    void getPartitionTypes( final Context ctx ) {
        ctx.json( Arrays.stream( PartitionType.values() ).filter( t -> t != PartitionType.NONE ).toArray( PartitionType[]::new ) );
    }


    private List<PartitionFunctionColumn> buildPartitionFunctionRow( PartitioningRequest request, List<PartitionFunctionInfoColumn> columnList ) {
        List<PartitionFunctionColumn> constructedRow = new ArrayList<>();

        for ( PartitionFunctionInfoColumn currentColumn : columnList ) {
            FieldType type;
            switch ( currentColumn.getFieldType() ) {
                case STRING:
                    type = FieldType.STRING;
                    break;
                case INTEGER:
                    type = FieldType.INTEGER;
                    break;
                case LIST:
                    type = FieldType.LIST;
                    break;
                case LABEL:
                    type = FieldType.LABEL;
                    break;
                default:
                    throw new RuntimeException( "Unknown Field ExpressionType: " + currentColumn.getFieldType() );
            }

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

        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace( request.schemaName );

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

        String content = "";
        for ( List<PartitionFunctionColumn> currentRow : request.rows ) {
            boolean rowSeparationApplied = false;
            for ( PartitionFunctionColumn currentColumn : currentRow ) {
                if ( currentColumn.modifiable ) {
                    // If more than one row, keep appending ','
                    if ( !rowSeparationApplied && request.rows.indexOf( currentRow ) != 0 ) {
                        content = content + functionInfo.getRowSeparation();
                        rowSeparationApplied = true;
                    }
                    content = content + currentColumn.sqlPrefix + " " + currentColumn.value + " " + currentColumn.sqlSuffix;
                }
            }
        }

        content = functionInfo.getSqlPrefix() + " " + content + " " + functionInfo.getSqlSuffix();

        //INFO - do discuss
        //Problem is that we took the structure completely out of the original JSON therefore losing valuable information and context
        //what part of rows were actually needed to build the SQL and which one not.
        //Now we have to crosscheck every statement
        //Actually to complex and rather poor maintenance quality.
        //Changes to extensions to this model now have to be made on two parts

        String query = String.format( "ALTER TABLE \"%s\".\"%s\" PARTITION BY %s (\"%s\") %s ",
                request.schemaName, request.tableName, request.functionName, request.partitionColumnName, content );

        Transaction trx = getTransaction();
        try {
            int i = executeSqlUpdate( trx, query );
            trx.commit();
            ctx.json( Result.builder().affectedRows( i ).generatedQuery( query ).build() );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Could not partition table", e );
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( query ).build() );
        }
    }


    void mergePartitions( final Context ctx ) {
        PartitioningRequest request = ctx.bodyAsClass( PartitioningRequest.class );
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" MERGE PARTITIONS", request.schemaName, request.tableName );
        Transaction trx = getTransaction();
        try {
            int i = executeSqlUpdate( trx, query );
            trx.commit();
            ctx.json( Result.builder().affectedRows( i ).generatedQuery( query ).build() );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Could not merge partitions", e );
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( query ).build() );
        }
    }


    void modifyPartitions( final Context ctx ) {
        ModifyPartitionRequest request = ctx.bodyAsClass( ModifyPartitionRequest.class );
        StringJoiner partitions = new StringJoiner( "," );
        for ( String partition : request.partitions ) {
            partitions.add( "\"" + partition + "\"" );
        }
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" MODIFY PARTITIONS(%s) ON STORE %s", request.schemaName, request.tableName, partitions.toString(), request.storeUniqueName );
        Transaction trx = getTransaction();
        try {
            int i = executeSqlUpdate( trx, query );
            trx.commit();
            ctx.json( Result.builder().affectedRows( i ).generatedQuery( query ).build() );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Could not modify partitions", e );
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( query ).build() );
        }
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
    void getAvailableStoresForIndexes( final Context ctx, Gson gson ) {
        Index index = ctx.bodyAsClass( Index.class );
        Placement dataPlacements = getPlacements( index );
        ImmutableMap<String, DataStore<?>> stores = AdapterManager.getInstance().getStores();
        //see https://stackoverflow.com/questions/18857884/how-to-convert-arraylist-of-custom-class-to-jsonarray-in-java
        JsonArray jsonArray = gson.toJsonTree( stores.values().stream().filter( ( s ) -> {
            if ( s.getAvailableIndexMethods() == null || s.getAvailableIndexMethods().size() == 0 ) {
                return false;
            }
            return dataPlacements.stores.stream().anyMatch( ( dp ) -> dp.uniqueName.equals( s.getUniqueName() ) );
        } ).toArray( DataStore[]::new ) ).getAsJsonArray();
        if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ) {
            JsonObject pdbFakeStore = new JsonObject();
            pdbFakeStore.addProperty( "uniqueName", "Polypheny-DB" );
            pdbFakeStore.add( "availableIndexMethods", gson.toJsonTree( IndexManager.getAvailableIndexMethods() ) );
            jsonArray.add( pdbFakeStore );
        }
        ctx.json( jsonArray );
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
                AdapterManager.getInstance().getStore( adapter.getAdapterId() ).updateSettings( adapter.getCurrentSettings() );
            } else if ( adapter instanceof DataSource ) {
                AdapterManager.getInstance().getSource( adapter.getAdapterId() ).updateSettings( adapter.getCurrentSettings() );
            }
            Catalog.getInstance().commit();
        } catch ( Throwable t ) {
            ctx.json( Result.builder().error( "Could not update AdapterSettings: " + t.getMessage() ).build() );
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
            ctx.json( Result.builder().error( "Error while resetting caches: " + e.getMessage() ).build() );
            return;
        }

        ctx.json( Result.builder().affectedRows( 1 ).build() );
    }


    /**
     * Get available adapters
     */
    private void getAvailableAdapters( Context ctx, AdapterType adapterType ) {
        List<AdapterInformation> adapters = AdapterManager.getInstance().getAvailableAdapters( adapterType );
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
    void addAdapter( final Context ctx, Gson gson ) throws ServletException, IOException {
        initMultipart( ctx );
        String body = "";
        Map<String, InputStream> inputStreams = new HashMap<>();

        for ( Part part : ctx.req.getParts() ) {
            if ( part.getName().equals( "body" ) ) {
                body = IOUtils.toString( ctx.req.getPart( "body" ).getInputStream(), StandardCharsets.UTF_8 );
            } else {
                inputStreams.put( part.getName(), part.getInputStream() );
            }
        }

        AdapterModel a = gson.fromJson( body, AdapterModel.class );
        Map<String, String> settings = new HashMap<>();

        ConnectionMethod method = ConnectionMethod.UPLOAD;
        if ( a.settings.containsKey( "method" ) ) {
            method = a.settings.get( "method" ).equals( "link" ) ? LINK : ConnectionMethod.UPLOAD;
        }

        for ( Entry<String, AbstractAdapterSetting> entry : a.settings.entrySet() ) {
            if ( entry.getValue() instanceof AbstractAdapterSettingDirectory ) {
                AbstractAdapterSettingDirectory setting = ((AbstractAdapterSettingDirectory) entry.getValue());
                if ( method == LINK ) {
                    Exception e = handleLinkFiles( ctx, a, setting, a.settings );
                    if ( e != null ) {
                        ctx.json( new Result( e ) );
                        return;
                    }
                } else {
                    handleUploadFiles( inputStreams, a, setting );
                }
                settings.put( entry.getKey(), entry.getValue().getValue() );
            } else {
                settings.put( entry.getKey(), entry.getValue().getValue() );
            }
        }

        String query = String.format( "ALTER ADAPTERS ADD \"%s\" USING '%s' AS '%s' WITH '%s'", a.uniqueName, a.adapterName, a.adapterType, Crud.gson.toJson( settings ) );
        Transaction transaction = getTransaction();
        try {
            int numRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            ctx.json( Result.builder().affectedRows( numRows ).generatedQuery( query ).build() );
        } catch ( Throwable e ) {
            log.error( "Could not deploy data store", e );
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( query ).build() );
        }
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
            return new RuntimeException( "Security check for access was not performed; id missing." );
        }
        Path path = Path.of( settings.get( "directoryName" ).defaultValue );
        if ( !SecurityManager.getInstance().checkPathAccess( path ) ) {
            return new RuntimeException( "Security check for access was not successful; not enough permissions." );
        }

        return null;
    }


    private static void handleUploadFiles( Map<String, InputStream> inputStreams, AdapterModel a, AbstractAdapterSettingDirectory setting ) {
        for ( String fileName : setting.fileNames ) {
            setting.inputStreams.put( fileName, inputStreams.get( fileName ) );
        }
        File path = PolyphenyHomeDirManager.getInstance().registerNewFolder( "data/csv/" + a.uniqueName );
        for ( Entry<String, InputStream> is : setting.inputStreams.entrySet() ) {
            try {
                File file = new File( path, is.getKey() );
                FileUtils.copyInputStreamToFile( is.getValue(), file );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
        setting.setDirectory( path.getAbsolutePath() );
    }


    /**
     * Remove an existing store or source
     */
    void removeAdapter( final Context ctx ) {
        String uniqueName = ctx.body();
        String query = String.format( "ALTER ADAPTERS DROP \"%s\"", uniqueName );
        Transaction transaction = getTransaction();
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            ctx.json( Result.builder().affectedRows( a ).generatedQuery( query ).build() );
        } catch ( TransactionException | QueryExecutionException e ) {
            log.error( "Could not remove store {}", ctx.body(), e );
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( query ).build() );
        }
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
            qim.addQueryInterface( catalog, request.clazzName, request.uniqueName, request.currentSettings );
            ctx.json( Result.builder().affectedRows( 1 ).generatedQuery( generatedQuery ).build() );
        } catch ( RuntimeException e ) {
            log.error( "Exception while deploying query interface", e );
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( generatedQuery ).build() );
        }
    }


    void updateQueryInterfaceSettings( final Context ctx ) {
        QueryInterfaceModel request = ctx.bodyAsClass( QueryInterfaceModel.class );
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        try {
            qim.getQueryInterface( request.uniqueName ).updateSettings( request.currentSettings );
            ctx.json( Result.builder().affectedRows( 1 ).build() );
        } catch ( Exception e ) {
            ctx.json( Result.builder().error( e.getMessage() ).build() );
        }
    }


    void removeQueryInterface( final Context ctx ) {
        String uniqueName = ctx.body();
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        String generatedQuery = String.format( "ALTER INTERFACES DROP \"%s\"", uniqueName );
        try {
            qim.removeQueryInterface( catalog, uniqueName );
            ctx.json( Result.builder().affectedRows( 1 ).generatedQuery( generatedQuery ).build() );
        } catch ( RuntimeException e ) {
            log.error( "Could not remove query interface {}", ctx.body(), e );
            ctx.json( Result.builder().error( e.getMessage() ).generatedQuery( generatedQuery ).build() );
        }
    }


    /**
     * Get the required information for the uml view: Foreign keys, Tables with its columns
     */
    void getUml( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        ArrayList<ForeignKey> fKeys = new ArrayList<>();
        ArrayList<DbTable> tables = new ArrayList<>();

        LogicalRelSnapshot relSnapshot = catalog.getSnapshot().rel();
        LogicalNamespace namespace = catalog.getSnapshot().getNamespace( request.schema );
        List<LogicalTable> catalogEntities = relSnapshot.getTablesFromNamespace( namespace.id );

        for ( LogicalTable catalogTable : catalogEntities ) {
            if ( catalogTable.entityType == EntityType.ENTITY || catalogTable.entityType == EntityType.SOURCE ) {
                // get foreign keys
                List<LogicalForeignKey> foreignKeys = catalog.getSnapshot().rel().getForeignKeys( catalogTable.id );
                for ( LogicalForeignKey logicalForeignKey : foreignKeys ) {
                    for ( int i = 0; i < logicalForeignKey.getReferencedKeyColumnNames().size(); i++ ) {
                        fKeys.add( ForeignKey.builder()
                                .targetSchema( logicalForeignKey.getReferencedKeySchemaName() )
                                .targetTable( logicalForeignKey.getReferencedKeyTableName() )
                                .targetColumn( logicalForeignKey.getReferencedKeyColumnNames().get( i ) )
                                .sourceSchema( logicalForeignKey.getSchemaName() )
                                .sourceTable( logicalForeignKey.getTableName() )
                                .sourceColumn( logicalForeignKey.getColumnNames().get( i ) )
                                .fkName( logicalForeignKey.name )
                                .onUpdate( logicalForeignKey.updateRule.toString() )
                                .onDelete( logicalForeignKey.deleteRule.toString() )
                                .build() );
                    }
                }

                // get tables with its columns
                DbTable table = new DbTable( catalogTable.name, catalog.getSnapshot().getNamespace( catalogTable.namespaceId ).getName(), catalogTable.modifiable, catalogTable.entityType );

                for ( LogicalColumn column : relSnapshot.getColumns( catalogTable.id ) ) {
                    table.addColumn( DbColumn.builder().name( column.name ).build() );
                }

                // get primary key with its columns
                if ( catalogTable.primaryKey != null ) {
                    LogicalPrimaryKey catalogPrimaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
                    for ( String columnName : catalogPrimaryKey.getColumnNames() ) {
                        table.addPrimaryKeyField( columnName );
                    }
                }

                // get unique constraints
                List<LogicalConstraint> logicalConstraints = catalog.getSnapshot().rel().getConstraints( catalogTable.id );
                for ( LogicalConstraint logicalConstraint : logicalConstraints ) {
                    if ( logicalConstraint.type == ConstraintType.UNIQUE ) {
                        // TODO: unique constraints can be over multiple columns.
                        if ( logicalConstraint.key.getColumnNames().size() == 1 &&
                                logicalConstraint.key.getSchemaName().equals( table.getSchema() ) &&
                                logicalConstraint.key.getTableName().equals( table.getTableName() ) ) {
                            table.addUniqueColumn( logicalConstraint.key.getColumnNames().get( 0 ) );
                        }
                        // table.addUnique( new ArrayList<>( catalogConstraint.key.columnNames ));
                    }
                }

                // get unique indexes
                List<LogicalIndex> logicalIndices = catalog.getSnapshot().rel().getIndexes( catalogTable.id, true );
                for ( LogicalIndex logicalIndex : logicalIndices ) {
                    // TODO: unique indexes can be over multiple columns.
                    if ( logicalIndex.key.getColumnNames().size() == 1 &&
                            logicalIndex.key.getSchemaName().equals( table.getSchema() ) &&
                            logicalIndex.key.getTableName().equals( table.getTableName() ) ) {
                        table.addUniqueColumn( logicalIndex.key.getColumnNames().get( 0 ) );
                    }
                    // table.addUnique( new ArrayList<>( catalogIndex.key.columnNames ));
                }

                tables.add( table );
            }
        }

        ctx.json( new Uml( tables, fKeys ) );
    }


    /**
     * Add foreign key
     */
    void addForeignKey( final Context ctx ) {
        ForeignKey fk = ctx.bodyAsClass( ForeignKey.class );
        Transaction transaction = getTransaction();

        String[] t = fk.getSourceTable().split( "\\." );
        String fkTable = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        t = fk.getTargetTable().split( "\\." );
        String pkTable = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result result;
        String sql = String.format( "ALTER TABLE %s ADD CONSTRAINT \"%s\" FOREIGN KEY (\"%s\") REFERENCES %s(\"%s\") ON UPDATE %s ON DELETE %s",
                fkTable, fk.getFkName(), fk.getSourceColumn(), pkTable, fk.getTargetColumn(), fk.getOnUpdate(), fk.getOnDelete() );
        try {
            executeSqlUpdate( transaction, sql );
            transaction.commit();
            result = Result.builder().affectedRows( 1 ).generatedQuery( sql ).build();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while adding a foreign key", e );
            result = Result.builder().error( e.getMessage() ).generatedQuery( sql ).build();
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
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
    Result executeRelAlg( final RelAlgRequest request, Session session ) {
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
            return Result.builder().error( e.getMessage() ).build();
        }

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = result.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( IntStream.range( 0, rowType.getFieldCount() ).boxed().collect( Collectors.toList() ), rowType.getFieldNames() );
        final AlgCollation collation =
                result instanceof Sort
                        ? ((Sort) result).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( result, result.getRowType(), Kind.SELECT, fields, collation );

        // Prepare
        PolyImplementation<PolyValue> polyImplementation = statement.getQueryProcessor().prepareQuery( root, true );

        if ( request.createView ) {

            String viewName = request.viewName;
            boolean replace = false;
            String viewType;

            if ( request.freshness != null ) {
                viewType = "Materialized View";
                DataStore<?> store = (DataStore<?>) AdapterManager.getInstance().getAdapter( request.store );
                List<DataStore<?>> stores = new ArrayList<>();
                stores.add( store );

                PlacementType placementType = store == null ? PlacementType.AUTOMATIC : PlacementType.MANUAL;

                List<String> columns = new ArrayList<>();
                root.alg.getRowType().getFieldList().forEach( f -> columns.add( f.getName() ) );

                //default Schema
                long schemaId = transaction.getDefaultSchema().id;

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
                        schemaId,
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
                root.alg.getRowType().getFieldList().forEach( f -> columns.add( f.getName() ) );

                // Default Schema
                long schemaId = transaction.getDefaultSchema().id;

                Gson gson = new Gson();

                DdlManager.getInstance().createView(
                        viewName,
                        schemaId,
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
                throw new RuntimeException( e );
            }

            return Result.builder().generatedQuery( "Created " + viewType + " \"" + viewName + "\" from logical query plan" ).build();
        }

        List<List<PolyValue>> rows;
        try {
            rows = polyImplementation.getRows( statement, getPageSize(), true, false );
        } catch ( Exception e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            return Result.builder().error( e.getMessage() ).build();
        }

        DbColumn[] header = new DbColumn[polyImplementation.getRowType().getFieldCount()];
        int counter = 0;
        for ( AlgDataTypeField col : polyImplementation.getRowType().getFieldList() ) {
            header[counter++] = DbColumn.builder()
                    .name( col.getName() )
                    .dataType( col.getType()
                            .getFullTypeString() ).nullable( col.getType()
                            .isNullable() == (ResultSetMetaData.columnNullable == 1) ).precision( col.getType()
                            .getPrecision() ).build();
        }

        List<String[]> data = computeResultData( rows, Arrays.asList( header ), statement.getTransaction() );

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
            throw new RuntimeException( e );
        }
        Result finalResult = Result.builder()
                .header( header )
                .data( data.toArray( new String[0][] ) )
                .xid( transaction.getXid().toString() )
                .generatedQuery( "Execute logical query plan" )
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
     * Create or drop a schema
     */
    void schemaRequest( final Context ctx ) {
        Schema schema = ctx.bodyAsClass( Schema.class );
        Transaction transaction = getTransaction();

        NamespaceType type = schema.getType();

        if ( type == NamespaceType.GRAPH ) {
            handleGraphDdl( schema, transaction, ctx );
            return;
        }

        // create schema
        if ( schema.isCreate() && !schema.isDrop() ) {

            StringBuilder query = new StringBuilder( "CREATE " );
            if ( schema.getType() == NamespaceType.DOCUMENT ) {
                query.append( "DOCUMENT " );
            }
            query.append( "SCHEMA " );

            query.append( "\"" ).append( schema.getName() ).append( "\"" );
            if ( schema.getAuthorization() != null && !schema.getAuthorization().equals( "" ) ) {
                query.append( " AUTHORIZATION " ).append( schema.getAuthorization() );
            }
            try {
                int rows = executeSqlUpdate( transaction, query.toString() );
                transaction.commit();
                ctx.json( Result.builder().affectedRows( rows ).build() );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while creating a schema", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( Result.builder().error( e.getMessage() ).build() );
            }
        }
        // drop schema
        else if ( !schema.isCreate() && schema.isDrop() ) {
            if ( type == null ) {
                List<LogicalNamespace> namespaces = catalog.getSnapshot().getNamespaces( new org.polypheny.db.catalog.logistic.Pattern( schema.getName() ) );
                assert namespaces.size() == 1;
                type = namespaces.get( 0 ).namespaceType;

                if ( type == NamespaceType.GRAPH ) {
                    handleGraphDdl( schema, transaction, ctx );
                    return;
                }
            }

            StringBuilder query = new StringBuilder( "DROP SCHEMA " );
            query.append( "\"" ).append( schema.getName() ).append( "\"" );
            if ( schema.isCascade() ) {
                query.append( " CASCADE" );
            }
            try {
                int rows = executeSqlUpdate( transaction, query.toString() );
                transaction.commit();
                ctx.json( Result.builder().affectedRows( rows ).build() );
            } catch ( TransactionException | QueryExecutionException e ) {
                log.error( "Caught exception while dropping a schema", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( Result.builder().error( e.getMessage() ).build() );
            }
        } else {
            ctx.json( Result.builder().error( "Neither the field 'create' nor the field 'drop' was set." ).build() );
        }
    }


    private void handleGraphDdl( Schema schema, Transaction transaction, Context ctx ) {
        if ( schema.isCreate() && !schema.isDrop() ) {
            Statement statement = transaction.createStatement();
            Processor processor = transaction.getProcessor( QueryLanguage.from( "cypher" ) );

            String query = String.format( "CREATE DATABASE %s", schema.getName() );

            List<? extends Node> nodes = processor.parse( query );
            ExtendedQueryParameters parameters = new ExtendedQueryParameters( query, NamespaceType.GRAPH, schema.getName() );
            try {
                PolyImplementation result = processor.prepareDdl( statement, nodes.get( 0 ), parameters );
                int rowsChanged = result.getRowsChanged( statement );
                transaction.commit();
                ctx.json( Result.builder().affectedRows( rowsChanged ).build() );
            } catch ( TransactionException | Exception e ) {
                log.error( "Caught exception while creating a graph namespace", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( Result.builder().error( e.getMessage() ).build() );
            }
        } else if ( schema.isDrop() && !schema.isCreate() ) {
            Statement statement = transaction.createStatement();
            Processor processor = transaction.getProcessor( QueryLanguage.from( "cypher" ) );

            String query = String.format( "DROP DATABASE %s", schema.getName() );

            List<? extends Node> nodes = processor.parse( query );
            ExtendedQueryParameters parameters = new ExtendedQueryParameters( query, NamespaceType.GRAPH, schema.getName() );
            try {
                PolyImplementation result = processor.prepareDdl( statement, nodes.get( 0 ), parameters );
                int rowsChanged = result.getRowsChanged( statement );
                transaction.commit();
                ctx.json( Result.builder().affectedRows( rowsChanged ).build() );
            } catch ( TransactionException | Exception e ) {
                log.error( "Caught exception while dropping a graph namespace", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( Result.builder().error( e.getMessage() ).build() );
            }
        } else {
            ctx.json( Result.builder().error( "Neither the field 'create' nor the field 'drop' was set." ).build() );
        }
    }


    /**
     * Get all supported data types of the DBMS.
     */
    public void getTypeInfo( final Context ctx ) {
        ctx.json( PolyType.availableTypes().toArray( new PolyType[0] ) );
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
        ArrayList<SidebarElement> nodes = new ArrayList<>();
        for ( InformationPage page : pages ) {
            nodes.add( new SidebarElement( page.getId(), page.getName(), NamespaceType.RELATIONAL, analyzerId + "/", page.getIcon() ).setLabel( page.getLabel() ) );
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
        getFile( ctx, ".polypheny/tmp/", true );
    }


    private File getFile( Context ctx, String location, boolean sendBack ) {
        String fileName = ctx.pathParam( "file" );
        File f = new File( System.getProperty( "user.home" ), location + fileName );
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
                if ( group2 != null && !group2.equals( "" ) ) {
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
        String zipFileName = UUID.randomUUID().toString() + ".zip";
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
     * Execute a select statement with default limit
     */
    public ResultBuilder<?, ?> executeSqlSelect( final Statement statement, final UIRequest request, final String sqlSelect ) throws QueryExecutionException {
        return executeSqlSelect( statement, request, sqlSelect, false, this );
    }


    public static ResultBuilder<?, ?> executeSqlSelect( final Statement statement, final UIRequest request, final String sqlSelect, final boolean noLimit, Crud crud ) throws QueryExecutionException {
        PolyImplementation<PolyValue> result;
        List<List<PolyValue>> rows;
        boolean hasMoreRows;
        boolean isAnalyze = statement.getTransaction().isAnalyze();

        try {
            result = crud.processQuery( statement, sqlSelect, isAnalyze );
            rows = result.getRows( statement, noLimit ? -1 : crud.getPageSize(), true, isAnalyze );
            hasMoreRows = result.hasMoreRows();

        } catch ( Throwable t ) {
            if ( statement.getTransaction().isAnalyze() ) {
                InformationManager analyzer = statement.getTransaction().getQueryAnalyzer();
                InformationPage exceptionPage = new InformationPage( "Stacktrace" ).fullWidth();
                InformationGroup exceptionGroup = new InformationGroup( exceptionPage.getId(), "Stacktrace" );
                InformationStacktrace exceptionElement = new InformationStacktrace( t, exceptionGroup );
                analyzer.addPage( exceptionPage );
                analyzer.addGroup( exceptionGroup );
                analyzer.registerInformation( exceptionElement );
            }
            throw new QueryExecutionException( t );
        }

        LogicalTable catalogTable = null;
        if ( request.tableId != null ) {
            String[] t = request.tableId.split( "\\." );

            catalogTable = crud.catalog.getSnapshot().rel().getTable( t[0], t[1] ).orElseThrow();
        }

        ArrayList<DbColumn> header = new ArrayList<>();
        for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
            String columnName = metaData.getName();

            String filter = "";
            if ( request.filter != null && request.filter.containsKey( columnName ) ) {
                filter = request.filter.get( columnName );
            }

            SortState sort;
            if ( request.sortState != null && request.sortState.containsKey( columnName ) ) {
                sort = request.sortState.get( columnName );
            } else {
                sort = new SortState();
            }

            DbColumn dbCol = DbColumn.builder()
                    .name( metaData.getName() )
                    .dataType( metaData.getType().getPolyType().getTypeName() )
                    .nullable( metaData.getType().isNullable() == (ResultSetMetaData.columnNullable == 1) )
                    .precision( metaData.getType().getPrecision() )
                    .sort( sort )
                    .filter( filter ).build();

            // Get column default values
            if ( catalogTable != null ) {
                Optional<LogicalColumn> logicalColumn = crud.catalog.getSnapshot().rel().getColumn( catalogTable.id, columnName );
                if ( logicalColumn.isPresent() ) {
                    if ( logicalColumn.get().defaultValue != null ) {
                        dbCol.defaultValue = logicalColumn.get().defaultValue.value;
                    }
                }
            }
            header.add( dbCol );
        }

        List<String[]> data = computeResultData( rows, header, statement.getTransaction() );

        return Result.builder().header( header.toArray( new DbColumn[0] ) ).data( data.toArray( new String[0][] ) ).namespaceType( result.getNamespaceType() ).language( QueryLanguage.from( "sql" ) ).affectedRows( data.size() ).hasMoreRows( hasMoreRows );
    }


    /**
     * Convert data from a query result to Strings readable in the UI
     *
     * @param rows Rows from the enumerable iterator
     * @param header Header from the UI-ResultSet
     */
    public static List<String[]> computeResultData( final List<List<PolyValue>> rows, final List<DbColumn> header, final Transaction transaction ) {
        ArrayList<String[]> data = new ArrayList<>();
        for ( List<PolyValue> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( PolyValue o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    /*switch ( header.get( counter ).dataType ) {
                        case "TIMESTAMP":
                            if ( o instanceof Long ) {
                                temp[counter] = DateTimeStringUtils.longToAdjustedString( (long) o, PolyType.TIMESTAMP );// TimestampString.fromMillisSinceEpoch( (long) o ).toString();
                            } else {
                                temp[counter] = o.toString();
                            }
                            break;
                        case "DATE":
                            if ( o instanceof Integer ) {
                                temp[counter] = DateTimeStringUtils.longToAdjustedString( (int) o, PolyType.DATE );//DateString.fromDaysSinceEpoch( (int) o ).toString();
                            } else {
                                temp[counter] = o.toString();
                            }
                            break;
                        case "TIME":
                            if ( o instanceof Integer ) {
                                temp[counter] = DateTimeStringUtils.longToAdjustedString( (int) o, PolyType.TIME );//TimeString.fromMillisOfDay( (int) o ).toString();
                            } else {
                                temp[counter] = o.toString();
                            }
                            break;
                        case "GRAPH NOT NULL":
                        case "NODE NOT NULL":
                        case "EDGE NOT NULL":
                            temp[counter] = ((GraphObject) o).toJson();
                            break;
                        case "FILE":
                        case "IMAGE":
                        case "AUDIO":
                        case "VIDEO":
                            String columnName = String.valueOf( header.get( counter ).name.hashCode() );
                            File mmFolder = new File( System.getProperty( "user.home" ), ".polypheny/tmp" );
                            mmFolder.mkdirs();
                            ContentInfoUtil util = new ContentInfoUtil();
                            if ( o instanceof File ) {
                                File f = ((File) o);
                                try {
                                    ContentInfo info = null;
                                    if ( !f.isDirectory() ) {
                                        info = util.findMatch( f );
                                    }
                                    String extension = "";
                                    if ( info != null && info.getFileExtensions() != null && info.getFileExtensions().length > 0 ) {
                                        extension = "." + info.getFileExtensions()[0];
                                    }
                                    File newLink = new File( mmFolder, columnName + "_" + f.getName() + extension );
                                    newLink.delete();//delete to override
                                    Path added;
                                    if ( f.isDirectory() && transaction.getInvolvedAdapters().stream().anyMatch( a -> a.getUniqueName().equals( "QFS" ) ) ) {
                                        added = Files.createSymbolicLink( newLink.toPath(), f.toPath() );
                                    } else if ( RuntimeConfig.UI_USE_HARDLINKS.getBoolean() && !f.isDirectory() ) {
                                        added = Files.createLink( newLink.toPath(), f.toPath() );
                                    } else {
                                        added = Files.copy( f.toPath(), newLink.toPath() );
                                        //added = Files.createSymbolicLink( newLink.toPath(), f.toPath() );
                                    }
                                    TemporalFileManager.addPath( transaction.getXid().toString(), added );
                                    temp[counter] = newLink.getName();
                                } catch ( Exception e ) {
                                    throw new RuntimeException( "Could not create link to mm file " + f.getAbsolutePath(), e );
                                }
                                break;
                            } else if ( o instanceof InputStream || o instanceof Blob ) {
                                InputStream is;
                                if ( o instanceof Blob ) {
                                    try {
                                        is = ((Blob) o).getBinaryStream();
                                    } catch ( SQLException e ) {
                                        throw new RuntimeException( "Could not get inputStream from Blob column", e );
                                    }
                                } else {
                                    is = (InputStream) o;
                                }
                                File f;
                                FileOutputStream fos = null;
                                try ( PushbackInputStream pbis = new PushbackInputStream( is, ContentInfoUtil.DEFAULT_READ_SIZE ) ) {
                                    byte[] buffer = new byte[ContentInfoUtil.DEFAULT_READ_SIZE];
                                    pbis.read( buffer );
                                    ContentInfo info = util.findMatch( buffer );
                                    pbis.unread( buffer );
                                    String extension = "";
                                    if ( info != null && info.getFileExtensions() != null && info.getFileExtensions().length > 0 ) {
                                        extension = "." + info.getFileExtensions()[0];
                                    }
                                    f = new File( mmFolder, columnName + "_" + UUID.randomUUID().toString() + extension );
                                    fos = new FileOutputStream( f.getPath() );
                                    IOUtils.copyLarge( pbis, fos );
                                    TemporalFileManager.addFile( transaction.getXid().toString(), f );
                                } catch ( IOException e ) {
                                    throw new RuntimeException( "Could not place file in mm folder", e );
                                } finally {
                                    if ( fos != null ) {
                                        try {
                                            fos.close();
                                        } catch ( IOException ignored ) {
                                            // ignore
                                        }
                                    }
                                }
                                temp[counter] = f.getName();
                                break;
                            } else if ( o instanceof byte[] || o instanceof Byte[] ) {
                                byte[] bytes;
                                if ( o instanceof byte[] ) {
                                    bytes = (byte[]) o;
                                } else {
                                    bytes = ArrayUtils.toPrimitive( (Byte[]) o );
                                }
                                ContentInfo info = util.findMatch( bytes );
                                String extension = "";
                                if ( info != null && info.getFileExtensions() != null && info.getFileExtensions().length > 0 ) {
                                    extension = "." + info.getFileExtensions()[0];
                                }
                                File f = new File( mmFolder, columnName + "_" + UUID.randomUUID().toString() + extension );
                                try ( FileOutputStream fos = new FileOutputStream( f ) ) {
                                    fos.write( bytes );
                                } catch ( IOException e ) {
                                    throw new RuntimeException( "Could not place file in mm folder", e );
                                }
                                temp[counter] = f.getName();
                                TemporalFileManager.addFile( transaction.getXid().toString(), f );
                                break;
                            }
                            //fall through
                        default:
                            temp[counter] = o.toString();
                    }*/
                    temp[counter] = o.toString();
                    if ( header.get( counter ).dataType.endsWith( "ARRAY" ) ) {
                        if ( o instanceof Array ) {
                            try {
                                temp[counter] = gson.toJson( ((Array) o).getArray(), Object[].class );
                            } catch ( SQLException sqlException ) {
                                temp[counter] = o.toString();
                            }
                        } else if ( o instanceof List ) {
                            // TODO js(knn): make sure all of this is not just a hotfix.
                            temp[counter] = gson.toJson( o );
                        } else {
                            temp[counter] = o.toString();
                        }
                    }
                    if ( header.get( counter ).dataType.contains( "Path" ) ) {
                        temp[counter] = ((GraphObject) o).toJson();
                    }
                }
                counter++;
            }
            data.add( temp );
        }
        return data;
    }


    private <T> PolyImplementation<T> processQuery( Statement statement, String sql, boolean isAnalyze ) {
        PolyImplementation<T> result;
        if ( isAnalyze ) {
            statement.getOverviewDuration().start( "Parsing" );
        }
        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "sql" ) );
        Node parsed = sqlProcessor.parse( sql ).get( 0 );
        if ( isAnalyze ) {
            statement.getOverviewDuration().stop( "Parsing" );
        }
        AlgRoot logicalRoot;
        QueryParameters parameters = new QueryParameters( sql, NamespaceType.RELATIONAL );
        if ( parsed.isA( Kind.DDL ) ) {
            result = sqlProcessor.prepareDdl( statement, parsed, parameters );
        } else {
            if ( isAnalyze ) {
                statement.getOverviewDuration().start( "Validation" );
            }
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            if ( isAnalyze ) {
                statement.getOverviewDuration().stop( "Validation" );
                statement.getOverviewDuration().start( "Translation" );
            }
            logicalRoot = sqlProcessor.translate( statement, validated.left, parameters );
            if ( isAnalyze ) {
                statement.getOverviewDuration().stop( "Translation" );
            }
            result = statement.getQueryProcessor().prepareQuery( logicalRoot, true );
        }
        return result;
    }


    public int executeSqlUpdate( final Transaction transaction, final String sqlUpdate ) throws QueryExecutionException {
        return executeSqlUpdate( transaction.createStatement(), transaction, sqlUpdate );
    }


    private int executeSqlUpdate( final Statement statement, final Transaction transaction, final String sqlUpdate ) throws QueryExecutionException {
        PolyImplementation<?> result;

        try {
            result = processQuery( statement, sqlUpdate, transaction.isAnalyze() );
        } catch ( Throwable t ) {
            if ( transaction.isAnalyze() ) {
                InformationManager analyzer = transaction.getQueryAnalyzer();
                InformationPage exceptionPage = new InformationPage( "Stacktrace" ).fullWidth();
                InformationGroup exceptionGroup = new InformationGroup( exceptionPage.getId(), "Stacktrace" );
                InformationStacktrace exceptionElement = new InformationStacktrace( t, exceptionGroup );
                analyzer.addPage( exceptionPage );
                analyzer.addGroup( exceptionGroup );
                analyzer.registerInformation( exceptionElement );
            }
            if ( t instanceof AvaticaRuntimeException ) {
                throw new QueryExecutionException( ((AvaticaRuntimeException) t).getErrorMessage(), t );
            } else {
                throw new QueryExecutionException( t.getMessage(), t );
            }

        }
        try {
            return result.getRowsChanged( statement );
        } catch ( Exception e ) {
            throw new QueryExecutionException( e );
        }
    }


    /**
     * Get the Number of rows in a table
     */
    private int getTableSize( Transaction transaction, final UIRequest request ) throws QueryExecutionException {
        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        String query = "SELECT count(*) FROM " + tableId;
        if ( request.filter != null ) {
            query += " " + filterTable( request.filter );
        }
        Statement statement = transaction.createStatement();
        Result result = executeSqlSelect( statement, request, query ).build();
        // We expect the result to be in the first column of the first row
        if ( result.data.length == 0 ) {
            return 0;
        } else {
            if ( statement.getMonitoringEvent() != null ) {
                StatementEvent eventData = statement.getMonitoringEvent();
                eventData.setRowCount( Integer.parseInt( result.data[0][0] ) );
            }
            return Integer.parseInt( result.getData()[0][0] );
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
            else if ( !entry.getValue().equals( "" ) ) {
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
        return getTransaction( analyze, useCache, transactionManager, userId, databaseId, "Polypheny-UI" );
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, TransactionManager transactionManager, long userId, long databaseId, String origin ) {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        Transaction transaction = transactionManager.startTransaction( snapshot.getUser( Catalog.defaultUserId ), snapshot.getNamespace( Catalog.defaultNamespaceId ), analyze, origin, MultimediaFlavor.FILE );
        transaction.setUseCache( useCache );
        return transaction;
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, Crud crud ) {
        return getTransaction( analyze, useCache, crud.transactionManager, crud.userId, crud.databaseId );
    }


    /**
     * Get the data types of each column of a table
     *
     * @param schemaName name of the schema
     * @param tableName name of the table
     * @return HashMap containing the type of each column. The key is the name of the column and the value is the Sql ExpressionType (java.sql.Types).
     */
    private Map<String, LogicalColumn> getCatalogColumns( String schemaName, String tableName ) {
        Map<String, LogicalColumn> dataTypes = new HashMap<>();

        LogicalTable table = getLogicalTable( schemaName, tableName );
        List<LogicalColumn> logicalColumns = catalog.getSnapshot().rel().getColumns( table.id );
        for ( LogicalColumn logicalColumn : logicalColumns ) {
            dataTypes.put( logicalColumn.name, logicalColumn );
        }

        return dataTypes;
    }


    void getTypeSchemas( final Context ctx ) {
        ctx.json( catalog
                .getSnapshot().
                getNamespaces( null )
                .stream()
                .collect( Collectors.toMap( LogicalNamespace::getName, LogicalNamespace::getNamespaceType ) ) );
    }


    /**
     * This method can be used to retrieve the status of a specific Docker instance and if
     * it is running correctly when using the provided settings
     */
    public void testDockerInstance( final Context ctx ) {
        String dockerIdAsString = ctx.pathParam( "dockerId" );
        int dockerId = Integer.parseInt( dockerIdAsString );

        ctx.json( DockerManager.getInstance().probeDockerStatus( dockerId ) );
    }


    /**
     * Retrieve a collection which maps the dockerInstance ids to the corresponding used ports
     */
    public void getUsedDockerPorts( final Context ctx ) {
        ctx.json( DockerManager.getInstance().getUsedPortsSorted() );
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
                throw new RuntimeException( e );
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
     * from https://stackoverflow.com/questions/2403830
     */
    private static void zipDirectory( String basePath, File dir, ZipOutputStream zipOut ) throws IOException {
        byte[] buffer = new byte[4096];
        File[] files = dir.listFiles();
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
                .collect( Collectors.toList() ) );
    }


    public static class QueryExecutionException extends Exception {

        QueryExecutionException( String message ) {
            super( message );
        }


        QueryExecutionException( String message, Throwable t ) {
            super( message, t );
        }


        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }

}
