/*
 * Copyright 2019-2022 The Polypheny Project
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


import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import io.javalin.http.Context;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.Blob;
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
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Part;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.Adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.Adapter.AbstractAdapterSettingDirectory;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
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
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.NameGenerator;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.cypher.cypher2alg.CypherQueryParameters;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
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
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.partition.PartitionFunctionInfo;
import org.polypheny.db.partition.PartitionFunctionInfo.PartitionFunctionInfoColumn;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.schema.graph.GraphObject;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.DateTimeStringUtils;
import org.polypheny.db.util.FileInputHandle;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.webui.SchemaToJsonMapper.JsonColumn;
import org.polypheny.db.webui.SchemaToJsonMapper.JsonTable;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.crud.StatisticCrud;
import org.polypheny.db.webui.models.AdapterModel;
import org.polypheny.db.webui.models.DbColumn;
import org.polypheny.db.webui.models.DbTable;
import org.polypheny.db.webui.models.ForeignKey;
import org.polypheny.db.webui.models.HubMeta;
import org.polypheny.db.webui.models.HubMeta.TableMapping;
import org.polypheny.db.webui.models.HubResult;
import org.polypheny.db.webui.models.Index;
import org.polypheny.db.webui.models.MaterializedInfos;
import org.polypheny.db.webui.models.PartitionFunctionModel;
import org.polypheny.db.webui.models.PartitionFunctionModel.FieldType;
import org.polypheny.db.webui.models.PartitionFunctionModel.PartitionFunctionColumn;
import org.polypheny.db.webui.models.Placement;
import org.polypheny.db.webui.models.Placement.RelationalStore;
import org.polypheny.db.webui.models.QueryInterfaceModel;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.ResultType;
import org.polypheny.db.webui.models.Schema;
import org.polypheny.db.webui.models.SidebarElement;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.Status;
import org.polypheny.db.webui.models.TableConstraint;
import org.polypheny.db.webui.models.Uml;
import org.polypheny.db.webui.models.UnderlyingTables;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest;
import org.polypheny.db.webui.models.requests.BatchUpdateRequest.Update;
import org.polypheny.db.webui.models.requests.ColumnRequest;
import org.polypheny.db.webui.models.requests.ConstraintRequest;
import org.polypheny.db.webui.models.requests.EditTableRequest;
import org.polypheny.db.webui.models.requests.HubRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest;
import org.polypheny.db.webui.models.requests.PartitioningRequest.ModifyPartitionRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.RelAlgRequest;
import org.polypheny.db.webui.models.requests.SchemaTreeRequest;
import org.polypheny.db.webui.models.requests.UIRequest;


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
     * Returns the content of a table with a maximum of PAGESIZE elements.
     */
    Result getTable( final UIRequest request ) {
        Transaction transaction = getTransaction();
        Result result;

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
            result = executeSqlSelect( transaction.createStatement(), request, query.toString(), request.noLimit );
            result.setXid( transaction.getXid().toString() );
        } catch ( Exception e ) {
            if ( request.filter != null ) {
                result = new Result( "Error while filtering table " + request.tableId );
            } else {
                result = new Result( "Could not fetch table " + request.tableId );
                log.error( "Caught exception while fetching a table", e );
            }
            try {
                transaction.rollback();
                return result;
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        // determine if it is a view or a table
        CatalogTable catalogTable;
        try {
            catalogTable = catalog.getTable( this.databaseId, t[0], t[1] );
            result.setNamespaceType( catalogTable.getNamespaceType() );
            if ( catalogTable.modifiable ) {
                result.setType( ResultType.TABLE );
            } else {
                result.setType( ResultType.VIEW );
            }
        } catch ( UnknownTableException e ) {
            log.error( "Caught exception", e );
            return result.setError( "Could not retrieve type of Result (table/view)." );
        }

        //get headers with default values
        ArrayList<DbColumn> cols = new ArrayList<>();
        ArrayList<String> primaryColumns;
        if ( catalogTable.primaryKey != null ) {
            CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
            primaryColumns = new ArrayList<>( primaryKey.getColumnNames() );
        } else {
            primaryColumns = new ArrayList<>();
        }
        for ( CatalogColumn catalogColumn : catalog.getColumns( catalogTable.id ) ) {
            String defaultValue = catalogColumn.defaultValue == null ? null : catalogColumn.defaultValue.value;
            String collectionsType = catalogColumn.collectionsType == null ? "" : catalogColumn.collectionsType.getName();
            cols.add(
                    new DbColumn(
                            catalogColumn.name,
                            catalogColumn.type.getName(),
                            collectionsType,
                            catalogColumn.nullable,
                            catalogColumn.length,
                            catalogColumn.scale,
                            catalogColumn.dimension,
                            catalogColumn.cardinality,
                            primaryColumns.contains( catalogColumn.name ),
                            defaultValue,
                            request.sortState == null ? new SortState() : request.sortState.get( catalogColumn.name ),
                            request.filter == null || request.filter.get( catalogColumn.name ) == null ? "" : request.filter.get( catalogColumn.name ) ) );
        }
        result.setHeader( cols.toArray( new DbColumn[0] ) );

        result.setCurrentPage( request.currentPage ).setTable( request.tableId );
        int tableSize = 0;
        try {
            tableSize = getTableSize( transaction, request );
        } catch ( Exception e ) {
            log.error( "Caught exception while determining page size", e );
        }
        result.setHighestPage( (int) Math.ceil( (double) tableSize / getPageSize() ) );
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
        return result;
    }


    void getSchemaTree( final Context ctx ) {
        SchemaTreeRequest request = ctx.bodyAsClass( SchemaTreeRequest.class );
        ArrayList<SidebarElement> result = new ArrayList<>();

        if ( request.depth < 1 ) {
            log.error( "Trying to fetch a schemaTree with depth < 1" );
            ctx.json( new ArrayList<>() );
        }

        List<CatalogSchema> schemas = catalog.getSchemas( databaseId, null );
        // remove unwanted namespaces
        schemas = schemas.stream().filter( s -> request.dataModels.contains( s.namespaceType ) ).collect( Collectors.toList() );
        for ( CatalogSchema schema : schemas ) {
            SidebarElement schemaTree = new SidebarElement( schema.name, schema.name, schema.namespaceType, "", getIconName( schema.namespaceType ) );

            if ( request.depth > 1 && schema.namespaceType != NamespaceType.GRAPH ) {
                ArrayList<SidebarElement> tableTree = new ArrayList<>();
                ArrayList<SidebarElement> viewTree = new ArrayList<>();
                ArrayList<SidebarElement> collectionTree = new ArrayList<>();
                List<CatalogTable> tables = catalog.getTables( schema.id, null );
                for ( CatalogTable table : tables ) {
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
                        List<CatalogColumn> columns = catalog.getColumns( table.id );
                        for ( CatalogColumn column : columns ) {
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
        Transaction transaction = getTransaction();
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        long schemaId = transaction.getDefaultSchema().id;
        String requestedSchema;
        if ( request.schema != null ) {
            requestedSchema = request.schema;
        } else {
            requestedSchema = catalog.getSchema( schemaId ).name;
        }

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        List<CatalogTable> tables = catalog.getTables( databaseId, new Catalog.Pattern( requestedSchema ), null );
        ArrayList<DbTable> result = new ArrayList<>();
        for ( CatalogTable t : tables ) {
            result.add( new DbTable( t.name, t.getNamespaceName(), t.modifiable, t.entityType ) );
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
            result = new Result( rows ).setGeneratedQuery( query );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            result = new Result( e );
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
            result = new Result( a ).setGeneratedQuery( query.toString() );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping or truncating a table", e );
            result = new Result( e ).setGeneratedQuery( query.toString() );
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
        query.append( colJoiner.toString() );
        query.append( ")" );
        if ( request.store != null && !request.store.equals( "" ) ) {
            query.append( String.format( " ON STORE \"%s\"", request.store ) );
        }

        try {
            int a = executeSqlUpdate( transaction, query.toString() );
            result = new Result( a ).setGeneratedQuery( query.toString() );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating a table", e );
            result = new Result( e ).setGeneratedQuery( query.toString() );
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
            ctx.json( new Result( e ) );
        }
        String[] split = tableId.split( "\\." );
        tableId = String.format( "\"%s\".\"%s\"", split[0], split[1] );

        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StringJoiner columns = new StringJoiner( ",", "(", ")" );
        StringJoiner values = new StringJoiner( ",", "(", ")" );

        List<CatalogColumn> catalogColumns = catalog.getColumns( new Catalog.Pattern( "APP" ), new Catalog.Pattern( split[0] ), new Catalog.Pattern( split[1] ), null );
        try {
            int i = 0;
            for ( CatalogColumn catalogColumn : catalogColumns ) {
                //part is null if it does not exist
                Part part = ctx.req.getPart( catalogColumn.name );
                if ( part == null ) {
                    //don't add if default value is set
                    if ( catalogColumn.defaultValue == null ) {
                        values.add( "NULL" );
                        columns.add( "\"" + catalogColumn.name + "\"" );
                    }
                } else {
                    columns.add( "\"" + catalogColumn.name + "\"" );
                    if ( part.getSubmittedFileName() == null ) {
                        String value = new BufferedReader( new InputStreamReader( part.getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
                        if ( catalogColumn.name.equals( "_id" ) ) {
                            if ( value.length() == 0 ) {
                                value = BsonUtil.getObjectId();
                            }
                        }
                        values.add( uiValueToSql( value, catalogColumn.type, catalogColumn.collectionsType ) );
                    } else {
                        values.add( "?" );
                        FileInputHandle fih = new FileInputHandle( statement, part.getInputStream() );
                        statement.getDataContext().addParameterValues( i++, catalogColumn.getAlgDataType( transaction.getTypeFactory() ), ImmutableList.of( fih ) );
                    }
                }
            }
        } catch ( IOException | ServletException e ) {
            log.error( "Could not generate INSERT statement", e );
            ctx.status( 500 );
            ctx.json( new Result( e ) );
        }

        String query = String.format( "INSERT INTO %s %s VALUES %s", tableId, columns.toString(), values.toString() );
        try {
            int numRows = executeSqlUpdate( statement, transaction, query );
            transaction.commit();
            ctx.json( new Result( numRows ).setGeneratedQuery( query ) );
        } catch ( Exception | TransactionException e ) {
            log.info( "Generated query: {}", query );
            log.error( "Could not insert row", e );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught error while rolling back transaction", e2 );
            }
            ctx.json( new Result( e ).setGeneratedQuery( query ) );
        }
    }


    /**
     * Run any query coming from the SQL console
     */
    public ArrayList<Result> anySqlQuery( final QueryRequest request, final Session session ) {
        Transaction transaction = getTransaction( request.analyze, request.cache );

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        ArrayList<Result> results = new ArrayList<>();
        boolean autoCommit = true;

        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( request.analyze ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( this );
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
                transaction = getTransaction( request.analyze, request.cache );
            }
            if ( Pattern.matches( "(?si:[\\s]*COMMIT.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    transaction.commit();
                    executionTime += System.nanoTime() - temp;
                    transaction = getTransaction( request.analyze, request.cache );
                    results.add( new Result().setGeneratedQuery( query ) );
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
                    transaction = getTransaction( request.analyze, request.cache );
                    results.add( new Result().setGeneratedQuery( query ) );
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
                    result = executeSqlSelect( transaction.createStatement(), request, query, noLimit )
                            .setGeneratedQuery( query )
                            .setXid( transaction.getXid().toString() );
                    executionTime += System.nanoTime() - temp;
                    results.add( result );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze, request.cache );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    if ( e.getCause() instanceof AvaticaRuntimeException ) {
                        result = new Result( ((AvaticaRuntimeException) e.getCause()).getErrorMessage() );
                    } else {
                        result = new Result( e.getCause().getMessage() );
                    }
                    result.setGeneratedQuery( query ).setXid( transaction.getXid().toString() );
                    results.add( result );
                    try {
                        transaction.rollback();
                    } catch ( TransactionException ex ) {
                        log.error( "Caught exception while rollback", e );
                    }
                }
            } else {
                try {
                    temp = System.nanoTime();
                    int numOfRows = executeSqlUpdate( transaction, query );
                    executionTime += System.nanoTime() - temp;

                    result = new Result( numOfRows ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() );
                    results.add( result );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze, request.cache );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() );
                    results.add( result );
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
            results.add( new Result( e ) );
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
     * Gets the classified Data from User
     * return possibly interesting Data to User
     */
    public void classifyData( final Context ctx ) {
        /*ClassifyAllData classifyAllData = ctx.bodyAsClass( ClassifyAllData.class );
        ExploreManager exploreManager = ExploreManager.getInstance();

        boolean isConvertedToSql = isClassificationToSql();

        Explore explore = exploreManager.classifyData( classifyAllData.id, classifyAllData.classified, isConvertedToSql );

        if ( isConvertedToSql ) {
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();
            Result result;

            try {
                result = executeSqlSelect( statement, classifyAllData, explore.getClassifiedSqlStatement(), false ).setGeneratedQuery( explore.getClassifiedSqlStatement() );
                transaction.commit();
            } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                log.error( "Caught exception while executing a query from the console", e );
                result = new Result( e ).setGeneratedQuery( explore.getClassifiedSqlStatement() );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rollback", ex );
                }
            }

            result.setExplorerId( explore.getId() );
            result.setCurrentPage( classifyAllData.cPage ).setTable( classifyAllData.tableId );

            result.setHighestPage( (int) Math.ceil( (double) explore.getTableSize() / getPageSize() ) );
            result.setClassificationInfo( "NoClassificationPossible" );
            result.setConvertedToSql( isConvertedToSql );

            ctx.json( result );
        } else {
            Result result = new Result( classifyAllData.header, Arrays.copyOfRange( explore.getData(), 0, 10 ) );

            result.setClassificationInfo( "NoClassificationPossible" );
            result.setExplorerId( explore.getId() );

            result.setCurrentPage( classifyAllData.cPage ).setTable( classifyAllData.tableId );
            result.setHighestPage( (int) Math.ceil( (double) explore.getData().length / getPageSize() ) );
            result.setConvertedToSql( isConvertedToSql );
            ctx.json( result );
        }
*/
    }


    /**
     * For pagination within the Explore-by-Example table
     */
    public void getExploreTables( final Context ctx ) {
        /*ExploreTables exploreTables = ctx.bodyAsClass( ExploreTables.class );
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();

        Result result;
        ExploreManager exploreManager = ExploreManager.getInstance();
        Explore explore = exploreManager.getExploreInformation( exploreTables.id );
        String[][] paginationData;

        String query = explore.getSqlStatement() + " OFFSET " + ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize());

        if ( !explore.isConvertedToSql() && !explore.isClassificationPossible() ) {
            int tablesize = explore.getData().length;

            if ( tablesize >= ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize()) && tablesize < ((Math.max( 0, exploreTables.cPage )) * getPageSize()) ) {
                paginationData = Arrays.copyOfRange( explore.getData(), ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize()), tablesize );
            } else {
                paginationData = Arrays.copyOfRange( explore.getData(), ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize()), ((Math.max( 0, exploreTables.cPage )) * getPageSize()) );
            }
            result = new Result( exploreTables.columns, paginationData );
            result.setClassificationInfo( "NoClassificationPossible" );
            result.setExplorerId( explore.getId() );

            result.setCurrentPage( exploreTables.cPage ).setTable( exploreTables.tableId );
            result.setHighestPage( (int) Math.ceil( (double) tablesize / getPageSize() ) );

            ctx.json( result );
        }

        try {
            result = executeSqlSelect( statement, exploreTables, query );
        } catch ( QueryExecutionException e ) {
            log.error( "Caught exception while fetching a table", e );
            result = new Result( "Could not fetch table " + exploreTables.tableId );
            try {
                transaction.rollback();
                ctx.status( 500 ).json( result );
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while committing transaction", e );
        }
        result.setExplorerId( explore.getId() );
        result.setCurrentPage( exploreTables.cPage ).setTable( exploreTables.tableId );
        int tableSize = explore.getTableSize();

        result.setHighestPage( (int) Math.ceil( (double) tableSize / getPageSize() ) );

        if ( !explore.isClassificationPossible() ) {
            result.setClassificationInfo( "NoClassificationPossible" );
        } else {
            result.setClassificationInfo( "ClassificationPossible" );
        }
        result.setIncludesClassificationInfo( explore.isDataAfterClassification );

        if ( explore.isDataAfterClassification ) {
            int tablesize = explore.getDataAfterClassification().size();
            List<String[]> paginationDataList;
            if ( tablesize >= ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize()) && tablesize < ((Math.max( 0, exploreTables.cPage )) * getPageSize()) ) {
                paginationDataList = explore.getDataAfterClassification().subList( ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize()), tablesize );
            } else {
                paginationDataList = explore.getDataAfterClassification().subList( ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize()), ((Math.max( 0, exploreTables.cPage )) * getPageSize()) );
            }

            paginationData = new String[paginationDataList.size()][];
            for ( int i = 0; i < paginationDataList.size(); i++ ) {
                paginationData[i] = paginationDataList.get( i );
            }

            result.setClassifiedData( paginationData );
        }
        ctx.json( result );
*/
    }


    /**
     * Creates the initial query for the Explore-by-Example process
     */
    public void createInitialExploreQuery( final Context ctx ) {
        /*QueryExplorationRequest queryExplorationRequest = ctx.bodyAsClass( QueryExplorationRequest.class );
        ExploreManager exploreManager = ExploreManager.getInstance();

        Result result;

        Explore explore = exploreManager.createSqlQuery( null, queryExplorationRequest.query );
        if ( explore.getDataType() == null ) {
            ctx.status( 400 ).json( new Result( "Explore by Example is only available for tables with the following datatypes: VARCHAR, INTEGER, SMALLINT, TINYINT, BIGINT, DECIMAL" ) );
            return;
        }

        Transaction transaction = getTransaction( queryExplorationRequest.analyze, true, transactionManager, userId, databaseId, "Explore-by-Example" );
        Statement statement = transaction.createStatement();
        try {
            String query = explore.getSqlStatement();
            result = executeSqlSelect( statement, queryExplorationRequest, query, false ).setGeneratedQuery( query );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", ex );
            }
        }

        result.setExplorerId( explore.getId() );
        if ( !explore.isClassificationPossible() ) {
            result.setClassificationInfo( "NoClassificationPossible" );

        } else {
            result.setClassificationInfo( "ClassificationPossible" );
        }
        result.setCurrentPage( queryExplorationRequest.cPage ).setTable( queryExplorationRequest.tableId );
        result.setHighestPage( (int) Math.ceil( (double) explore.getTableSize() / getPageSize() ) );

        ctx.json( result );*/
    }


    /**
     * Start Classification, classifies the initial dataset, to show what would be within the final result set
     */
    public void exploration( final Context ctx ) {
        /*ExploreData exploreData = ctx.bodyAsClass( ExploreData.class );

        String[] dataType = new String[exploreData.header.length + 1];
        for ( int i = 0; i < exploreData.header.length; i++ ) {
            dataType[i] = exploreData.header[i].dataType;
        }
        dataType[exploreData.header.length] = "VARCHAR";

        ExploreManager e = ExploreManager.getInstance();
        Explore explore = e.exploreData( exploreData.id, exploreData.classified, dataType );

        ctx.json( new ExploreResult( exploreData.header, explore.getDataAfterClassification(), explore.getId(), explore.getBuildGraph() ) );*/
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
     * @param tableName Table name
     * @param columnName Column name
     * @param filter Filter. Key: column name, value: the value of the entry, e.g. 1 or abc or [1,2,3] or {@code null}
     */
    private String computeWherePK( final String tableName, final String columnName, final Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", "", "" );
        Map<String, CatalogColumn> catalogColumns = getCatalogColumns( tableName, columnName );
        CatalogTable catalogTable;
        try {
            catalogTable = catalog.getTable( databaseId, tableName, columnName );
            CatalogPrimaryKey pk = catalog.getPrimaryKey( catalogTable.primaryKey );
            for ( long colId : pk.columnIds ) {
                String colName = catalog.getColumn( colId ).name;
                String condition;
                if ( filter.containsKey( colName ) ) {
                    String val = filter.get( colName );
                    CatalogColumn col = catalogColumns.get( colName );
                    condition = uiValueToSql( val, col.type, col.collectionsType );
                    condition = String.format( "\"%s\" = %s", colName, condition );
                    joiner.add( condition );
                }
            }
        } catch ( UnknownTableException e ) {
            throw new RuntimeException( "Error while deriving PK WHERE condition", e );
        }
        return " WHERE " + joiner.toString();
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
            result = new Result( numOfRows );
        } catch ( TransactionException | Exception e ) {
            log.error( "Caught exception while deleting a row", e );
            result = new Result( e ).setGeneratedQuery( builder.toString() );
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
            ctx.json( new Result( e ) );
        }

        String[] split = tableId.split( "\\." );
        tableId = String.format( "\"%s\".\"%s\"", split[0], split[1] );

        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        StringJoiner setStatements = new StringJoiner( ",", "", "" );

        List<CatalogColumn> catalogColumns = catalog.getColumns( new Catalog.Pattern( "APP" ), new Catalog.Pattern( split[0] ), new Catalog.Pattern( split[1] ), null );

        int i = 0;
        for ( CatalogColumn catalogColumn : catalogColumns ) {
            Part part = ctx.req.getPart( catalogColumn.name );
            if ( part == null ) {
                continue;
            }
            if ( part.getSubmittedFileName() == null ) {
                String value = new BufferedReader( new InputStreamReader( part.getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
                String parsed = gson.fromJson( value, String.class );
                if ( parsed == null ) {
                    setStatements.add( String.format( "\"%s\" = NULL", catalogColumn.name ) );
                } else {
                    setStatements.add( String.format( "\"%s\" = %s", catalogColumn.name, uiValueToSql( parsed, catalogColumn.type, catalogColumn.collectionsType ) ) );
                }
            } else {
                setStatements.add( String.format( "\"%s\" = ?", catalogColumn.name ) );
                FileInputHandle fih = new FileInputHandle( statement, part.getInputStream() );
                statement.getDataContext().addParameterValues( i++, null, ImmutableList.of( fih ) );
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
                result = new Result( numOfRows );
            } else {
                transaction.rollback();
                result = new Result( "Attempt to update " + numOfRows + " rows was blocked." );
                result.setGeneratedQuery( builder.toString() );
            }
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a row", e );
            result = new Result( e ).setGeneratedQuery( builder.toString() );
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
            ctx.json( new Result( totalRows ) );
        } catch ( ServletException | IOException | QueryExecutionException | TransactionException e ) {
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
                ctx.status( 500 ).json( new Result( e ) );
            }
            log.error( "The batch update failed", e );
            ctx.status( 500 ).json( new Result( e ) );
        }
    }


    /**
     * Get the columns of a table
     */
    void getColumns( final Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        Result result;

        String[] t = request.tableId.split( "\\." );
        ArrayList<DbColumn> cols = new ArrayList<>();

        try {
            CatalogTable catalogTable = catalog.getTable( databaseId, t[0], t[1] );
            ArrayList<String> primaryColumns;
            if ( catalogTable.primaryKey != null ) {
                CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                primaryColumns = new ArrayList<>( primaryKey.getColumnNames() );
            } else {
                primaryColumns = new ArrayList<>();
            }
            for ( CatalogColumn catalogColumn : catalog.getColumns( catalogTable.id ) ) {
                String defaultValue = catalogColumn.defaultValue == null ? null : catalogColumn.defaultValue.value;
                String collectionsType = catalogColumn.collectionsType == null ? "" : catalogColumn.collectionsType.getName();
                cols.add(
                        new DbColumn(
                                catalogColumn.name,
                                catalogColumn.type.getName(),
                                collectionsType,
                                catalogColumn.nullable,
                                catalogColumn.length,
                                catalogColumn.scale,
                                catalogColumn.dimension,
                                catalogColumn.cardinality,
                                primaryColumns.contains( catalogColumn.name ),
                                defaultValue ) );
            }
            result = new Result( cols.toArray( new DbColumn[0] ), null );
            if ( catalogTable.entityType == EntityType.ENTITY ) {
                result.setType( ResultType.TABLE );
            } else if ( catalogTable.entityType == EntityType.MATERIALIZED_VIEW ) {
                result.setType( ResultType.MATERIALIZED );
            } else {
                result.setType( ResultType.VIEW );
            }
        } catch ( UnknownTableException e ) {
            log.error( "Caught exception while getting a column", e );
            ctx.status( 400 ).json( new Result( e ) );
            return;
        }

        ctx.json( result );
    }


    void getDataSourceColumns( final Context ctx ) throws UnknownDatabaseException, UnknownTableException, UnknownSchemaException {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        CatalogTable catalogTable = catalog.getTable( "APP", request.getSchemaName(), request.getTableName() );

        if ( catalogTable.entityType == EntityType.VIEW ) {
            ImmutableMap<Long, ImmutableList<Long>> underlyingTable = ((CatalogView) catalogTable).getUnderlyingTables();

            List<DbColumn> columns = new ArrayList<>();
            for ( Long columnIds : catalogTable.fieldIds ) {
                CatalogColumn col = catalog.getColumn( columnIds );
                columns.add( new DbColumn(
                        col.name,
                        col.type.getName(),
                        col.collectionsType == null ? "" : col.collectionsType.getName(),
                        col.nullable,
                        col.length,
                        col.scale,
                        col.dimension,
                        col.cardinality,
                        false,
                        col.defaultValue == null ? null : col.defaultValue.value
                ).setPhysicalName( col.name ) );

            }
            ctx.json( new Result( columns.toArray( new DbColumn[0] ), null ).setType( ResultType.VIEW ) );
        } else {
            if ( catalog.getColumnPlacement( catalogTable.fieldIds.get( 0 ) ).size() != 1 ) {
                throw new RuntimeException( "The table has an unexpected number of placements!" );
            }

            int adapterId = catalog.getColumnPlacement( catalogTable.fieldIds.get( 0 ) ).get( 0 ).adapterId;
            CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
            List<String> pkColumnNames = primaryKey.getColumnNames();
            List<DbColumn> columns = new ArrayList<>();
            for ( CatalogColumnPlacement ccp : catalog.getColumnPlacementsOnAdapterPerTable( adapterId, catalogTable.id ) ) {
                CatalogColumn col = catalog.getColumn( ccp.columnId );
                columns.add( new DbColumn(
                        col.name,
                        col.type.getName(),
                        col.collectionsType == null ? "" : col.collectionsType.getName(),
                        col.nullable,
                        col.length,
                        col.scale,
                        col.dimension,
                        col.cardinality,
                        pkColumnNames.contains( col.name ),
                        col.defaultValue == null ? null : col.defaultValue.value
                ).setPhysicalName( ccp.physicalColumnName ) );
            }
            ctx.json( new Result( columns.toArray( new DbColumn[0] ), null ).setType( ResultType.TABLE ) );
        }
    }


    /**
     * Get additional columns of the DataSource that are not mapped to the table.
     */
    void getAvailableSourceColumns( final Context ctx ) throws UnknownDatabaseException, UnknownTableException, UnknownSchemaException {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        CatalogTable table = catalog.getTable( "APP", request.getSchemaName(), request.getTableName() );
        ImmutableMap<Integer, ImmutableList<Long>> placements = catalog.getColumnPlacementsByAdapter( table.id );
        Set<Integer> adapterIds = placements.keySet();
        if ( adapterIds.size() > 1 ) {
            log.warn( String.format( "The number of sources of an entity should not be > 1 (%s.%s)", request.getSchemaName(), request.getTableName() ) );
        }
        List<Result> exportedColumns = new ArrayList<>();
        for ( int adapterId : adapterIds ) {
            Adapter adapter = AdapterManager.getInstance().getAdapter( adapterId );
            if ( adapter instanceof DataSource ) {
                DataSource dataSource = (DataSource) adapter;
                for ( Entry<String, List<ExportedColumn>> entry : dataSource.getExportedColumns().entrySet() ) {
                    List<DbColumn> columnList = new ArrayList<>();
                    for ( ExportedColumn col : entry.getValue() ) {
                        DbColumn dbCol = new DbColumn(
                                col.name,
                                col.type.getName(),
                                col.collectionsType == null ? "" : col.collectionsType.getName(),
                                col.nullable,
                                col.length,
                                col.scale,
                                col.dimension,
                                col.cardinality,
                                col.primary,
                                null ).setPhysicalName( col.physicalColumnName );
                        columnList.add( dbCol );
                    }
                    exportedColumns.add( new Result( columnList.toArray( new DbColumn[0] ), null ).setTable( entry.getKey() ) );
                    columnList.clear();
                }
                ctx.json( exportedColumns.toArray( new Result[0] ) );
                return;
            }

        }

        ctx.json( new Result( "Could not retrieve exported source fields." ) );
    }


    void getMaterializedInfo( final Context ctx ) throws UnknownDatabaseException, UnknownTableException, UnknownSchemaException {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );

        CatalogTable catalogTable = catalog.getTable( databaseId, request.schema, request.table );

        if ( catalogTable.entityType == EntityType.MATERIALIZED_VIEW ) {
            CatalogMaterializedView catalogMaterializedView = (CatalogMaterializedView) catalogTable;

            MaterializedCriteria materializedCriteria = catalogMaterializedView.getMaterializedCriteria();

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

        result = new Result( 1 ).setGeneratedQuery( queries.toString() );
        try {
            for ( String q : queries ) {
                sBuilder.append( q );
                executeSqlUpdate( transaction, q );
            }
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a column", e );
            result = new Result( e ).setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught exception during rollback", e2 );
                result = new Result( e2 ).setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() );
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

        result = new Result( 1 ).setGeneratedQuery( queries.toString() );
        try {
            for ( String query : queries ) {
                sBuilder.append( query );
                executeSqlUpdate( transaction, query );
            }
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a column", e );
            result = new Result( e ).setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught exception during rollback", e2 );
                result = new Result( e2 ).setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() );
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
        Result result;
        try {
            int affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( affectedRows ).setGeneratedQuery( query );
        } catch ( TransactionException | QueryExecutionException e ) {
            log.error( "Caught exception while adding a column", e );
            result = new Result( e );
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
            result = new Result( affectedRows );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a column", e );
            result = new Result( e );
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
        ctx.json( new Result( new DbColumn[0], new String[][]{ data } ) );
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

        try {
            CatalogTable catalogTable = catalog.getTable( databaseId, t[0], t[1] );

            // get primary key
            if ( catalogTable.primaryKey != null ) {
                CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
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
            List<CatalogConstraint> constraints = catalog.getConstraints( catalogTable.id );
            for ( CatalogConstraint catalogConstraint : constraints ) {
                if ( catalogConstraint.type == ConstraintType.UNIQUE ) {
                    temp.put( catalogConstraint.name, new ArrayList<>( catalogConstraint.key.getColumnNames() ) );
                }
            }
            for ( Map.Entry<String, ArrayList<String>> entry : temp.entrySet() ) {
                resultList.add( new TableConstraint( entry.getKey(), "UNIQUE", entry.getValue() ) );
            }

            // the foreign keys are listed separately

            DbColumn[] header = { new DbColumn( "Name" ), new DbColumn( "Type" ), new DbColumn( "Columns" ) };
            ArrayList<String[]> data = new ArrayList<>();
            resultList.forEach( c -> data.add( c.asRow() ) );

            result = new Result( header, data.toArray( new String[0][2] ) );
        } catch ( UnknownTableException e ) {
            log.error( "Caught exception while fetching constraints", e );
            result = new Result( e );
        }

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
            result = new Result( rows );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a constraint", e );
            result = new Result( e );
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
        if ( request.constraint.columns.length > 0 ) {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( String s : request.constraint.columns ) {
                joiner.add( "\"" + s + "\"" );
            }
            String query = "ALTER TABLE " + tableId + " ADD PRIMARY KEY " + joiner.toString();
            try {
                int rows = executeSqlUpdate( transaction, query );
                transaction.commit();
                result = new Result( rows ).setGeneratedQuery( query );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while adding a primary key", e );
                result = new Result( e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
            }
        } else {
            result = new Result( "Cannot add primary key if no columns are provided." );
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
                result = new Result( rows ).setGeneratedQuery( query );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while adding a unique constraint", e );
                result = new Result( e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
            }
        } else {
            result = new Result( "Cannot add unique constraint if no columns are provided." );
        }
        ctx.json( result );
    }


    /**
     * Get indexes of a table
     */
    void getIndexes( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        Result result;
        try {
            CatalogTable catalogTable = catalog.getTable( databaseId, request.schema, request.table );
            List<CatalogIndex> catalogIndexes = catalog.getIndexes( catalogTable.id, false );

            DbColumn[] header = {
                    new DbColumn( "Name" ),
                    new DbColumn( "Columns" ),
                    new DbColumn( "Location" ),
                    new DbColumn( "Method" ),
                    new DbColumn( "Type" ) };

            ArrayList<String[]> data = new ArrayList<>();

            // Get explicit indexes
            for ( CatalogIndex catalogIndex : catalogIndexes ) {
                String[] arr = new String[5];
                String storeUniqueName;
                if ( catalogIndex.location == 0 ) {
                    // a polystore index
                    storeUniqueName = "Polypheny-DB";
                } else {
                    storeUniqueName = catalog.getAdapter( catalogIndex.location ).uniqueName;
                }
                arr[0] = catalogIndex.name;
                arr[1] = String.join( ", ", catalogIndex.key.getColumnNames() );
                arr[2] = storeUniqueName;
                arr[3] = catalogIndex.methodDisplayName;
                arr[4] = catalogIndex.type.name();
                data.add( arr );
            }

            // Get functional indexes
            for ( Integer storeId : catalogTable.dataPlacements ) {
                Adapter adapter = AdapterManager.getInstance().getAdapter( storeId );
                DataStore store;
                if ( adapter instanceof DataStore ) {
                    store = (DataStore) adapter;
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

            result = new Result( header, data.toArray( new String[0][2] ) );

        } catch ( UnknownTableException e ) {
            log.error( "Caught exception while fetching indexes", e );
            result = new Result( e );
        }
        ctx.json( result );
    }


    /**
     * Drop an index of a table
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
            result = new Result( a ).setGeneratedQuery( query );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping an index", e );
            result = new Result( e );
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
            result = new Result( a );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating an index", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        ctx.json( result );
    }


    void getUnderlyingTable( final Context ctx ) throws UnknownDatabaseException, UnknownTableException, UnknownSchemaException {

        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        CatalogTable catalogTable = catalog.getTable( "APP", request.getSchemaName(), request.getTableName() );

        if ( catalogTable.entityType == EntityType.VIEW ) {
            ImmutableMap<Long, ImmutableList<Long>> underlyingTableOriginal = ((CatalogView) catalogTable).getUnderlyingTables();
            Map<String, List<String>> underlyingTable = new HashMap<>();
            for ( Entry<Long, ImmutableList<Long>> entry : underlyingTableOriginal.entrySet() ) {
                List<String> columns = new ArrayList<>();
                for ( Long ids : entry.getValue() ) {
                    columns.add( catalog.getColumn( ids ).name );
                }
                underlyingTable.put( catalog.getTable( entry.getKey() ).name, columns );
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
        try {
            CatalogTable table = catalog.getTable( databaseId, schemaName, tableName );
            Placement p = new Placement( table.partitionProperty.isPartitioned, catalog.getPartitionGroupNames( table.id ), table.entityType );
            if ( table.entityType == EntityType.VIEW ) {

                return p;
            } else {
                long pkid = table.primaryKey;
                List<Long> pkColumnIds = Catalog.getInstance().getPrimaryKey( pkid ).columnIds;
                CatalogColumn pkColumn = Catalog.getInstance().getColumn( pkColumnIds.get( 0 ) );
                List<CatalogColumnPlacement> pkPlacements = catalog.getColumnPlacement( pkColumn.id );
                for ( CatalogColumnPlacement placement : pkPlacements ) {
                    Adapter adapter = AdapterManager.getInstance().getAdapter( placement.adapterId );
                    p.addAdapter( new RelationalStore(
                            adapter.getUniqueName(),
                            adapter.getAdapterName(),
                            catalog.getColumnPlacementsOnAdapterPerTable( adapter.getAdapterId(), table.id ),
                            catalog.getPartitionGroupsIndexOnDataPlacement( placement.adapterId, placement.tableId ),
                            table.partitionProperty.numPartitionGroups,
                            table.partitionProperty.partitionType ) );
                }
                return p;
            }
        } catch ( UnknownTableException e ) {
            log.error( "Caught exception while getting placements", e );
            return new Placement( e );
        }
    }


    /**
     * Add or drop a data placement.
     * Parameter of type models.Index: index name corresponds to storeUniqueName
     * Index method: either 'ADD' or 'DROP'
     */
    void addDropPlacement( final Context ctx ) {
        Index index = ctx.bodyAsClass( Index.class );
        if ( !index.getMethod().equalsIgnoreCase( "ADD" ) && !index.getMethod().equalsIgnoreCase( "DROP" ) && !index.getMethod().equalsIgnoreCase( "MODIFY" ) ) {
            ctx.json( new Result( "Invalid request" ) );
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
            ctx.json( new Result( e ) );
        }
        ctx.json( new Result( affectedRows ).setGeneratedQuery( query ) );
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

                //Used specifically for Temp-Partitioning since number of selected partitions remains 2 but chunks change
                //enables user to used selected "number of partitions" being used as default value for "number of interal data chunks"
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


    void getPartitionFunctionModel( final Context ctx ) throws UnknownDatabaseException, UnknownColumnException, UnknownTableException, UnknownSchemaException {
        PartitioningRequest request = ctx.bodyAsClass( PartitioningRequest.class );

        // Get correct partition function
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( request.method );

        // Check whether the selected partition function supports the selected partition column
        CatalogColumn partitionColumn;

        partitionColumn = Catalog.getInstance().getColumn( "APP", request.schemaName, request.tableName, request.column );

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
        PartitionManager partitionManager = null;
        try {
            partitionManager = partitionManagerFactory.getPartitionManager( PartitionType.getByName( request.functionName ) );
        } catch ( UnknownPartitionTypeException e ) {
            throw new RuntimeException( e );
        }

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
            ctx.json( new Result( i ).setGeneratedQuery( query ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Could not partition table", e );
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( new Result( e ).setGeneratedQuery( query ) );
        }
    }


    void mergePartitions( final Context ctx ) {
        PartitioningRequest request = ctx.bodyAsClass( PartitioningRequest.class );
        String query = String.format( "ALTER TABLE \"%s\".\"%s\" MERGE PARTITIONS", request.schemaName, request.tableName );
        Transaction trx = getTransaction();
        try {
            int i = executeSqlUpdate( trx, query );
            trx.commit();
            ctx.json( new Result( i ).setGeneratedQuery( query ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Could not merge partitions", e );
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( new Result( e ).setGeneratedQuery( query ) );
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
            ctx.json( new Result( i ).setGeneratedQuery( query ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Could not modify partitions", e );
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
            ctx.json( new Result( e ).setGeneratedQuery( query ) );
        }
    }


    /**
     * Get deployed data stores
     */
    void getStores( final Context ctx ) {
        ImmutableMap<String, DataStore> stores = AdapterManager.getInstance().getStores();
        DataStore[] out = stores.values().toArray( new DataStore[0] );
        ctx.json( out );
    }


    /**
     * Get the available stores on which a new index can be placed. 'Polypheny-DB' is part of the list, if polystore-indexes are enabled
     */
    void getAvailableStoresForIndexes( final Context ctx, Gson gson ) {
        Index index = ctx.bodyAsClass( Index.class );
        Placement dataPlacements = getPlacements( index );
        ImmutableMap<String, DataStore> stores = AdapterManager.getInstance().getStores();
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
        JsonDeserializer<Adapter> storeDeserializer = ( json, typeOfT, context ) -> {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get( "type" ).getAsString();
            try {
                return context.deserialize( jsonObject, Class.forName( type ) );
            } catch ( ClassNotFoundException cnfe ) {
                throw new JsonParseException( "Unknown element type: " + type, cnfe );
            }
        };
        Gson adapterGson = new GsonBuilder().registerTypeAdapter( Adapter.class, storeDeserializer ).create();
        Adapter adapter = adapterGson.fromJson( ctx.body(), Adapter.class );
        try {
            if ( adapter instanceof DataStore ) {
                AdapterManager.getInstance().getStore( adapter.getAdapterId() ).updateSettings( adapter.getCurrentSettings() );
            } else if ( adapter instanceof DataSource ) {
                AdapterManager.getInstance().getSource( adapter.getAdapterId() ).updateSettings( adapter.getCurrentSettings() );
            }
            Catalog.getInstance().commit();
        } catch ( Throwable t ) {
            ctx.json( new Result( "Could not update AdapterSettings", t ) );
            return;
        }

        // Reset caches (not a nice solution to create a transaction, statement and query processor for doing this but it
        // currently seams to be the best option). When migrating this to a DDL manager, make sure to find a better approach.
        Transaction transaction = null;
        try {
            transaction = getTransaction();
            transaction.createStatement().getQueryProcessor().resetCaches();
            transaction.commit();
        } catch ( TransactionException e ) {
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException transactionException ) {
                    log.error( "Exception while rollback", transactionException );
                }
            }
            ctx.json( new Result( "Error while resetting caches", e ) );
            return;
        }

        ctx.json( new Result( 1 ) );
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
        ImmutableMap<String, DataSource> sources = AdapterManager.getInstance().getSources();
        ctx.json( sources.values().toArray( new DataSource[0] ) );
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
        for ( Entry<String, AbstractAdapterSetting> entry : a.settings.entrySet() ) {
            if ( entry.getValue() instanceof AbstractAdapterSettingDirectory ) {
                AbstractAdapterSettingDirectory setting = ((AbstractAdapterSettingDirectory) entry.getValue());
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
                settings.put( entry.getKey(), entry.getValue().getValue() );
            } else {
                settings.put( entry.getKey(), entry.getValue().getValue() );
            }
        }

        String query = String.format( "ALTER ADAPTERS ADD \"%s\" USING '%s' WITH '%s'", a.uniqueName, a.clazzName, Crud.gson.toJson( settings ) );
        Transaction transaction = getTransaction();
        try {
            int numRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            ctx.json( new Result( numRows ).setGeneratedQuery( query ) );
        } catch ( Throwable e ) {
            log.error( "Could not deploy data store", e );
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
            ctx.json( new Result( e ).setGeneratedQuery( query ) );
        }
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
            ctx.json( new Result( a ).setGeneratedQuery( query ) );
        } catch ( TransactionException | QueryExecutionException e ) {
            log.error( "Could not remove store {}", ctx.body(), e );
            try {
                transaction.rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Exception while rollback", transactionException );
            }
            ctx.json( new Result( e ).setGeneratedQuery( query ) );
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
            ctx.json( new Result( 1 ).setGeneratedQuery( generatedQuery ) );
        } catch ( RuntimeException e ) {
            log.error( "Exception while deploying query interface", e );
            ctx.json( new Result( e ).setGeneratedQuery( generatedQuery ) );
        }
    }


    void updateQueryInterfaceSettings( final Context ctx ) {
        QueryInterfaceModel request = ctx.bodyAsClass( QueryInterfaceModel.class );
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        try {
            qim.getQueryInterface( request.uniqueName ).updateSettings( request.currentSettings );
            ctx.json( new Result( 1 ) );
        } catch ( Exception e ) {
            ctx.json( new Result( e ) );
        }
    }


    void removeQueryInterface( final Context ctx ) {
        String uniqueName = ctx.body();
        QueryInterfaceManager qim = QueryInterfaceManager.getInstance();
        String generatedQuery = String.format( "ALTER INTERFACES DROP \"%s\"", uniqueName );
        try {
            qim.removeQueryInterface( catalog, uniqueName );
            ctx.json( new Result( 1 ).setGeneratedQuery( generatedQuery ) );
        } catch ( RuntimeException | UnknownQueryInterfaceException e ) {
            log.error( "Could not remove query interface {}", ctx.body(), e );
            ctx.json( new Result( e ).setGeneratedQuery( generatedQuery ) );
        }
    }


    /**
     * Get the required information for the uml view: Foreign keys, Tables with its columns
     */
    void getUml( final Context ctx ) {
        EditTableRequest request = ctx.bodyAsClass( EditTableRequest.class );
        ArrayList<ForeignKey> fKeys = new ArrayList<>();
        ArrayList<DbTable> tables = new ArrayList<>();

        List<CatalogTable> catalogEntities = catalog.getTables( databaseId, new Catalog.Pattern( request.schema ), null );
        for ( CatalogTable catalogTable : catalogEntities ) {
            if ( catalogTable.entityType == EntityType.ENTITY || catalogTable.entityType == EntityType.SOURCE ) {
                // get foreign keys
                List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( catalogTable.id );
                for ( CatalogForeignKey catalogForeignKey : foreignKeys ) {
                    for ( int i = 0; i < catalogForeignKey.getReferencedKeyColumnNames().size(); i++ ) {
                        fKeys.add( ForeignKey.builder()
                                .targetSchema( catalogForeignKey.getReferencedKeySchemaName() )
                                .targetTable( catalogForeignKey.getReferencedKeyTableName() )
                                .targetColumn( catalogForeignKey.getReferencedKeyColumnNames().get( i ) )
                                .sourceSchema( catalogForeignKey.getSchemaName() )
                                .sourceTable( catalogForeignKey.getTableName() )
                                .sourceColumn( catalogForeignKey.getColumnNames().get( i ) )
                                .fkName( catalogForeignKey.name )
                                .onUpdate( catalogForeignKey.updateRule.toString() )
                                .onDelete( catalogForeignKey.deleteRule.toString() )
                                .build() );
                    }
                }

                // get tables with its columns
                DbTable table = new DbTable( catalogTable.name, catalogTable.getNamespaceName(), catalogTable.modifiable, catalogTable.entityType );
                for ( String columnName : catalogTable.getColumnNames() ) {
                    table.addColumn( new DbColumn( columnName ) );
                }

                // get primary key with its columns
                if ( catalogTable.primaryKey != null ) {
                    CatalogPrimaryKey catalogPrimaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                    for ( String columnName : catalogPrimaryKey.getColumnNames() ) {
                        table.addPrimaryKeyField( columnName );
                    }
                }

                // get unique constraints
                List<CatalogConstraint> catalogConstraints = catalog.getConstraints( catalogTable.id );
                for ( CatalogConstraint catalogConstraint : catalogConstraints ) {
                    if ( catalogConstraint.type == ConstraintType.UNIQUE ) {
                        // TODO: unique constraints can be over multiple columns.
                        if ( catalogConstraint.key.getColumnNames().size() == 1 &&
                                catalogConstraint.key.getSchemaName().equals( table.getSchema() ) &&
                                catalogConstraint.key.getTableName().equals( table.getTableName() ) ) {
                            table.addUniqueColumn( catalogConstraint.key.getColumnNames().get( 0 ) );
                        }
                        // table.addUnique( new ArrayList<>( catalogConstraint.key.columnNames ));
                    }
                }

                // get unique indexes
                List<CatalogIndex> catalogIndexes = catalog.getIndexes( catalogTable.id, true );
                for ( CatalogIndex catalogIndex : catalogIndexes ) {
                    // TODO: unique indexes can be over multiple columns.
                    if ( catalogIndex.key.getColumnNames().size() == 1 &&
                            catalogIndex.key.getSchemaName().equals( table.getSchema() ) &&
                            catalogIndex.key.getTableName().equals( table.getTableName() ) ) {
                        table.addUniqueColumn( catalogIndex.key.getColumnNames().get( 0 ) );
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
            result = new Result( 1 ).setGeneratedQuery( sql );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while adding a foreign key", e );
            result = new Result( e ).setGeneratedQuery( sql );
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
        Transaction transaction = getTransaction( request.analyze, request.useCache );
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
            return new Result( e );
        }

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = result.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                result instanceof Sort
                        ? ((Sort) result).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( result, result.getRowType(), Kind.SELECT, fields, collation );

        // Prepare
        PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( root, true );

        if ( request.createView ) {

            String viewName = request.viewName;
            boolean replace = false;
            String viewType;

            if ( request.freshness != null ) {
                viewType = "Materialized View";
                DataStore store = (DataStore) AdapterManager.getInstance().getAdapter( request.store );
                List<DataStore> stores = new ArrayList<>();
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

                try {
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
                            Catalog.QueryLanguage.REL_ALG,
                            false,
                            false
                    );
                } catch ( EntityAlreadyExistsException | GenericCatalogException | UnknownColumnException e ) {
                    log.error( "Not possible to create Materialized View because the name is already used", e );
                    Result finalResult = new Result( e );
                    finalResult.setGeneratedQuery( "Execute logical query plan" );
                    return finalResult;
                } catch ( ColumnNotExistsException | ColumnAlreadyExistsException e ) {
                    log.error( "Error while creating materialized view", e );
                    Result finalResult = new Result( e );
                    finalResult.setGeneratedQuery( "Execute logical query plan" );
                    return finalResult;
                }

            } else {

                viewType = "View";
                List<DataStore> store = null;
                PlacementType placementType = store == null ? PlacementType.AUTOMATIC : PlacementType.MANUAL;

                List<String> columns = new ArrayList<>();
                root.alg.getRowType().getFieldList().forEach( f -> columns.add( f.getName() ) );

                // Default Schema
                long schemaId = transaction.getDefaultSchema().id;

                Gson gson = new Gson();

                try {
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
                            Catalog.QueryLanguage.REL_ALG
                    );
                } catch ( EntityAlreadyExistsException | GenericCatalogException | UnknownColumnException e ) {
                    log.error( "Not possible to create View because the Name is already used", e );
                    Result finalResult = new Result( e );
                    finalResult.setGeneratedQuery( "Execute logical query plan" );
                    return finalResult;
                }

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

            return new Result().setGeneratedQuery( "Created " + viewType + " \"" + viewName + "\" from logical query plan" );
        }

        List<List<Object>> rows;
        try {
            rows = polyImplementation.getRows( statement, getPageSize(), true, false );
        } catch ( Exception e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            return new Result( e );
        }

        DbColumn[] header = new DbColumn[polyImplementation.getRowType().getFieldCount()];
        int counter = 0;
        for ( AlgDataTypeField col : polyImplementation.getRowType().getFieldList() ) {
            header[counter++] = new DbColumn(
                    col.getName(),
                    col.getType().getFullTypeString(),
                    col.getType().isNullable() == (ResultSetMetaData.columnNullable == 1),
                    col.getType().getPrecision(),
                    null,
                    null );
        }

        ArrayList<String[]> data = computeResultData( rows, Arrays.asList( header ), statement.getTransaction() );

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
        Result finalResult = new Result( header, data.toArray( new String[0][] ) ).setXid( transaction.getXid().toString() );

        finalResult.setGeneratedQuery( "Execute logical query plan" );

        if ( queryAnalyzer != null ) {
            InformationPage p1 = new InformationPage( "Query analysis", "Analysis of the query." );
            InformationGroup g1 = new InformationGroup( p1, "Execution time" );
            InformationText text;
            if ( executionTime < 1e4 ) {
                text = new InformationText( g1, String.format( "Execution time: %d nanoseconds", executionTime ) );
            } else {
                long millis = TimeUnit.MILLISECONDS.convert( executionTime, TimeUnit.NANOSECONDS );
                // format time: see: https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java#answer-625444
                //noinspection SuspiciousDateFormat
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
                ctx.json( new Result( rows ) );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while creating a schema", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( new Result( e ) );
            }
        }
        // drop schema
        else if ( !schema.isCreate() && schema.isDrop() ) {
            if ( type == null ) {
                List<CatalogSchema> namespaces = catalog.getSchemas( Catalog.defaultDatabaseId, new Catalog.Pattern( schema.getName() ) );
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
                ctx.json( new Result( rows ) );
            } catch ( TransactionException | QueryExecutionException e ) {
                log.error( "Caught exception while dropping a schema", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( new Result( e ) );
            }
        } else {
            ctx.json( new Result( "Neither the field 'create' nor the field 'drop' was set." ) );
        }
    }


    private void handleGraphDdl( Schema schema, Transaction transaction, Context ctx ) {
        if ( schema.isCreate() && !schema.isDrop() ) {
            Statement statement = transaction.createStatement();
            Processor processor = transaction.getProcessor( QueryLanguage.CYPHER );

            String query = String.format( "CREATE DATABASE %s", schema.getName() );

            List<? extends Node> nodes = processor.parse( query );
            CypherQueryParameters parameters = new CypherQueryParameters( query, NamespaceType.GRAPH, schema.getName() );
            try {
                PolyImplementation result = processor.prepareDdl( statement, nodes.get( 0 ), parameters );
                int rowsChanged = result.getRowsChanged( statement );
                transaction.commit();
                ctx.json( new Result( rowsChanged ) );
            } catch ( TransactionException | Exception e ) {
                log.error( "Caught exception while creating a graph namespace", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( new Result( e ) );
            }
        } else if ( schema.isDrop() && !schema.isCreate() ) {
            Statement statement = transaction.createStatement();
            Processor processor = transaction.getProcessor( QueryLanguage.CYPHER );

            String query = String.format( "DROP DATABASE %s", schema.getName() );

            List<? extends Node> nodes = processor.parse( query );
            CypherQueryParameters parameters = new CypherQueryParameters( query, NamespaceType.GRAPH, schema.getName() );
            try {
                PolyImplementation result = processor.prepareDdl( statement, nodes.get( 0 ), parameters );
                int rowsChanged = result.getRowsChanged( statement );
                transaction.commit();
                ctx.json( new Result( rowsChanged ) );
            } catch ( TransactionException | Exception e ) {
                log.error( "Caught exception while dropping a graph namespace", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                ctx.json( new Result( e ) );
            }
        } else {
            ctx.json( new Result( "Neither the field 'create' nor the field 'drop' was set." ) );
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
        ForeignKeyOption[] options = Catalog.ForeignKeyOption.values();
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
        WebSocket.sendMessage( session, this.gson.toJson( nodes.toArray( new SidebarElement[0] ) ) );
    }


    /**
     * Get the content of an InformationPage of a query analyzer.
     */
    public void getAnalyzerPage( final Context ctx ) {
        String[] params = ctx.bodyAsClass( String[].class );
        ctx.json( InformationManager.getInstance( params[0] ).getPage( params[1] ) );
    }


    /**
     * Import a dataset from Polypheny-Hub into Polypheny-DB
     */
    void importDataset( final Context ctx ) {
        HubRequest request = ctx.bodyAsClass( HubRequest.class );
        String error = null;

        String randomFileName = UUID.randomUUID().toString();
        final String sysTempDir = System.getProperty( "java.io.tmpdir" );
        final File tempDir = new File( sysTempDir + File.separator + "hub" + File.separator + randomFileName + File.separator );
        if ( !tempDir.mkdirs() ) { // create folder
            log.error( "Unable to create temp folder: {}", tempDir.getAbsolutePath() );
            ctx.json( new HubResult( "Unable to create temp folder" ) );
            return;
        }

        // see: https://www.baeldung.com/java-download-file
        final File zipFile = new File( tempDir, "import.zip" );
        Transaction transaction = null;
        try (
                BufferedInputStream in = new BufferedInputStream( new URL( request.url ).openStream() );
                FileOutputStream fos = new FileOutputStream( zipFile )
        ) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ( (bytesRead = in.read( dataBuffer, 0, 1024 )) != -1 ) {
                fos.write( dataBuffer, 0, bytesRead );
            }

            // extract zip, see https://www.baeldung.com/java-compress-and-uncompress
            dataBuffer = new byte[1024];
            final File extractedFolder = new File( tempDir, "import" );
            if ( !extractedFolder.mkdirs() ) {
                log.error( "Unable to create folder for extracting files: {}", tempDir.getAbsolutePath() );
                ctx.json( new HubResult( "Unable to create folder for extracting files" ) );
                return;
            }
            try ( ZipInputStream zis = new ZipInputStream( new FileInputStream( zipFile ) ) ) {
                ZipEntry zipEntry = zis.getNextEntry();
                while ( zipEntry != null ) {
                    File newFile = new File( extractedFolder, zipEntry.getName() );
                    try ( FileOutputStream fosEntry = new FileOutputStream( newFile ) ) {
                        int len;
                        while ( (len = zis.read( dataBuffer )) > 0 ) {
                            fosEntry.write( dataBuffer, 0, len );
                        }
                    }
                    zipEntry = zis.getNextEntry();
                }
            }

            // delete .zip after unzipping
            if ( !zipFile.delete() ) {
                log.error( "Unable to delete zip file: {}", zipFile.getAbsolutePath() );
            }

            Status status = new Status( "tableImport", request.tables.size() );
            int ithTable = 0;
            for ( TableMapping m : request.tables.values() ) {
                //create table from json
                Path jsonPath = Paths.get( new File( extractedFolder, m.initialName + ".json" ).getPath() );
                String json = new String( Files.readAllBytes( jsonPath ), StandardCharsets.UTF_8 );
                JsonTable table = gson.fromJson( json, JsonTable.class );
                String newName = m.newName != null ? m.newName : table.tableName;
                assert (table.tableName.equals( m.initialName ));

                transaction = getTransaction();
                HubResult createdTableError = createTableFromJson( json, newName, request, transaction );
                transaction.commit();
                if ( createdTableError != null ) {
                    transaction.rollback();
                    ctx.json( createdTableError );
                    return;
                    //todo check
                }

                // Import data from .csv file
                transaction = getTransaction();
                importCsvFile( m.initialName + ".csv", table, transaction, extractedFolder, request, newName, status, ithTable++ );
                transaction.commit();
            }
        } catch ( IOException | TransactionException e ) {
            log.error( "Could not import dataset", e );
            error = "Could not import dataset" + e.getMessage();
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rolling back transaction", e );
                }
            }
        } catch ( QueryExecutionException e ) {
            log.error( "Could not create table from imported json file", e );
            error = "Could not create table from imported json file" + e.getMessage();
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rolling back transaction", e );
                }
            }
            //} catch ( CsvValidationException | GenericCatalogException e ) {
        } finally {
            // Delete temp folder
            if ( !deleteDirectory( tempDir ) ) {
                log.error( "Unable to delete temp folder: {}", tempDir.getAbsolutePath() );
            }
        }

        if ( error != null ) {
            ctx.json( new HubResult( error ) );
        } else {
            ctx.json( new HubResult().setMessage( String.format( "Imported dataset into entity %s on store %s", request.schema, request.store ) ) );
        }
    }


    private HubResult createTableFromJson( final String json, final String newName, final HubRequest request, final Transaction transaction ) throws QueryExecutionException {
        // create table from .json file
        List<CatalogTable> tablesInSchema = catalog.getTables( this.databaseId, new Catalog.Pattern( request.schema ), null );
        int tableAlreadyExists = (int) tablesInSchema.stream().filter( t -> t.name.equals( newName ) ).count();
        if ( tableAlreadyExists > 0 ) {
            return new HubResult( String.format( "Cannot import the dataset since the schema '%s' already contains a entity with the name '%s'", request.schema, newName ) );
        }

        String createTable = SchemaToJsonMapper.getCreateTableStatementFromJson( json, request.createPks, request.defaultValues, request.schema, newName, request.store );
        executeSqlUpdate( transaction, createTable );
        return null;
    }


    private void importCsvFile( final String csvFileName, final JsonTable table, final Transaction transaction, final File extractedFolder, final HubRequest request, final String tableName, final Status status, final int ithTable ) throws IOException, QueryExecutionException {
        StringJoiner columnJoiner = new StringJoiner( ",", "(", ")" );
        for ( JsonColumn col : table.getColumns() ) {
            columnJoiner.add( "\"" + col.columnName + "\"" );
        }
        String columns = columnJoiner.toString();
        StringJoiner valueJoiner = new StringJoiner( ",", "VALUES", "" );
        StringJoiner rowJoiner;

        //see https://www.callicoder.com/java-read-write-csv-file-opencsv/

        final int BATCH_SIZE = RuntimeConfig.HUB_IMPORT_BATCH_SIZE.getInteger();
        long csvCounter = 0;
        try (
                Reader reader = new BufferedReader( new FileReader( new File( extractedFolder, csvFileName ) ) );
                CSVReader csvReader = new CSVReader( reader )
        ) {
            long lineCount = Files.lines( new File( extractedFolder, csvFileName ).toPath() ).count();
            String[] nextRecord;
            while ( (nextRecord = csvReader.readNext()) != null ) {
                rowJoiner = new StringJoiner( ",", "(", ")" );
                for ( int i = 0; i < table.getColumns().size(); i++ ) {
                    if ( PolyType.get( table.getColumns().get( i ).type ).getFamily() == PolyTypeFamily.CHARACTER ) {
                        rowJoiner.add( "'" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else if ( PolyType.get( table.getColumns().get( i ).type ) == PolyType.DATE ) {
                        rowJoiner.add( "date '" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else if ( PolyType.get( table.getColumns().get( i ).type ) == PolyType.TIME ) {
                        rowJoiner.add( "time '" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else if ( PolyType.get( table.getColumns().get( i ).type ) == PolyType.TIMESTAMP ) {
                        rowJoiner.add( "timestamp '" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else {
                        rowJoiner.add( nextRecord[i] );
                    }
                }
                valueJoiner.add( rowJoiner.toString() );
                csvCounter++;
                if ( csvCounter % BATCH_SIZE == 0 && csvCounter != 0 ) {
                    String insertQuery = String.format( "INSERT INTO \"%s\".\"%s\" %s %s", request.schema, tableName, columns, valueJoiner.toString() );
                    executeSqlUpdate( transaction, insertQuery );
                    valueJoiner = new StringJoiner( ",", "VALUES", "" );
                    status.setStatus( csvCounter, lineCount, ithTable );
                    WebSocket.broadcast( gson.toJson( status, Status.class ) );
                }
            }
            if ( csvCounter % BATCH_SIZE != 0 ) {
                String insertQuery = String.format( "INSERT INTO \"%s\".\"%s\" %s %s", request.schema, tableName, columns, valueJoiner.toString() );
                executeSqlUpdate( transaction, insertQuery );
                status.setStatus( csvCounter, lineCount, ithTable );
                WebSocket.broadcast( gson.toJson( status, Status.class ) );
            }
        }
    }


    /**
     * Export a table into a .zip consisting of a json file containing information of the table and columns and a csv files with the data
     */
    void exportTable( final Context ctx ) {
        HubRequest request = ctx.bodyAsClass( HubRequest.class );
        Transaction transaction = getTransaction( false, true );
        Statement statement = transaction.createStatement();
        HubMeta metaData = new HubMeta( request.schema );

        String randomFileName = UUID.randomUUID().toString();
        final Charset charset = StandardCharsets.UTF_8;
        final String sysTempDir = System.getProperty( "java.io.tmpdir" );
        final File tempDir = new File( sysTempDir + File.separator + "hub" + File.separator + randomFileName + File.separator );
        if ( !tempDir.mkdirs() ) { // create folder
            log.error( "Unable to create temp folder: {}", tempDir.getAbsolutePath() );
            ctx.json( new Result( "Unable to create temp folder" ) );
            return;
        }
        File tableFile;
        File catalogFile;
        ArrayList<File> tableFiles = new ArrayList<>();
        ArrayList<File> catalogFiles = new ArrayList<>();
        final int BATCH_SIZE = RuntimeConfig.HUB_IMPORT_BATCH_SIZE.getInteger();
        int ithTable = 0;
        Status status = new Status( "tableExport", request.tables.size() );
        try {
            for ( TableMapping table : request.tables.values() ) {
                tableFile = new File( tempDir, table.initialName + ".csv" );
                catalogFile = new File( tempDir, table.initialName + ".json" );
                tableFiles.add( tableFile );
                catalogFiles.add( catalogFile );
                OutputStreamWriter catalogWriter = new OutputStreamWriter( new FileOutputStream( catalogFile ), charset );
                FileOutputStream tableStream = new FileOutputStream( tableFile );
                log.info( String.format( "Exporting %s.%s", request.schema, table.initialName ) );
                CatalogTable catalogTable = catalog.getTable( this.databaseId, request.schema, table.initialName );

                catalogWriter.write( SchemaToJsonMapper.exportTableDefinitionAsJson( catalogTable, request.createPks, request.defaultValues ) );
                catalogWriter.flush();
                catalogWriter.close();

                String query = String.format( "SELECT * FROM \"%s\".\"%s\"", request.schema, table.initialName );
                // TODO use iterator instead of Result
                Result tableData = executeSqlSelect( statement, new UIRequest(), query, true );

                int totalRows = tableData.getData().length;
                int counter = 0;
                for ( String[] row : tableData.getData() ) {
                    int cols = row.length;
                    for ( int i = 0; i < cols; i++ ) {
                        if ( row[i].contains( "\n" ) ) {
                            String line = String.format( "\"%s\"", row[i] );
                            tableStream.write( line.getBytes( charset ) );
                        } else {
                            tableStream.write( row[i].getBytes( charset ) );
                        }
                        if ( i != cols - 1 ) {
                            tableStream.write( ",".getBytes( charset ) );
                        } else {
                            tableStream.write( "\n".getBytes( charset ) );
                        }
                    }
                    counter++;
                    if ( counter % BATCH_SIZE == 0 ) {
                        status.setStatus( counter, totalRows, ithTable );
                        WebSocket.broadcast( gson.toJson( status, Status.class ) );
                    }
                }
                status.setStatus( counter, totalRows, ithTable );
                WebSocket.broadcast( gson.toJson( status, Status.class ) );
                tableStream.flush();
                tableStream.close();
                metaData.addTable( table.initialName, counter );
                ithTable++;
            }
            status.complete();

            File zipFile = new File( tempDir, "table.zip" );
            FileOutputStream zipStream = new FileOutputStream( zipFile );
            //from https://www.baeldung.com/java-compress-and-uncompress
            ArrayList<File> allFiles = new ArrayList<>( tableFiles );
            allFiles.addAll( catalogFiles );
            try ( ZipOutputStream zipOut = new ZipOutputStream( zipStream, charset ) ) {
                for ( File fileToZip : allFiles ) {
                    try ( FileInputStream fis = new FileInputStream( fileToZip ) ) {
                        ZipEntry zipEntry = new ZipEntry( fileToZip.getName() );
                        zipOut.putNextEntry( zipEntry );

                        byte[] bytes = new byte[1024];
                        int length;
                        while ( (length = fis.read( bytes )) >= 0 ) {
                            zipOut.write( bytes, 0, length );
                        }
                    }
                }
                zipOut.finish();
            }
            zipStream.close();

            metaData.setFileSize( zipFile.length() );
            File metaFile = new File( tempDir, "meta.json" );
            FileOutputStream metaOutputStream = new FileOutputStream( metaFile );
            metaOutputStream.write( gson.toJson( metaData, HubMeta.class ).getBytes() );
            metaOutputStream.flush();
            metaOutputStream.close();

            //send file to php backend using Unirest
            HttpResponse<String> jsonResponse = Unirest.post( request.hubLink )
                    .field( "action", "uploadDataset" )
                    .field( "userId", String.valueOf( request.userId ) )
                    .field( "secret", request.secret )
                    .field( "name", request.name )
                    .field( "description", request.description )
                    .field( "pub", String.valueOf( request.pub ) )
                    .field( "dataset", zipFile )
                    .field( "metaData", metaFile )
                    .asString();

            // Get result
            String resultString = jsonResponse.getBody();
            log.info( String.format( "Exported %s.[%s]", request.schema, request.tables.values().stream().map( n -> n.initialName ).collect( Collectors.joining( "," ) ) ) );

            try {
                ctx.result( resultString );
            } catch ( JsonSyntaxException e ) {
                ctx.json( new Result( resultString ) );
            }
        } catch ( IOException e ) {
            log.error( "Failed to write temporary file", e );
            ctx.status( 400 ).json( "Failed to write temporary file" );
        } catch ( Exception e ) {
            log.error( "Error while exporting table", e );
            ctx.status( 400 ).json( new Result( "Error while exporting table" ) );
        } finally {
            // delete temp folder
            if ( !deleteDirectory( tempDir ) ) {
                log.error( "Unable to delete temp folder: {}", tempDir.getAbsolutePath() );
            }
            try {
                transaction.commit();
            } catch ( TransactionException e ) {
                log.error( "Error while fetching table", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException transactionException ) {
                    log.error( "Exception while rollback", transactionException );
                }
            }
        }
    }


    void getFile( final Context ctx ) {
        String fileName = ctx.pathParam( "file" );
        File f = new File( System.getProperty( "user.home" ), ".polypheny/tmp/" + fileName );
        if ( !f.exists() ) {
            ctx.status( 404 );
            ctx.result( "" );
            return;
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
            ctx.res.setContentLengthLong( (int) fileLength );
            try ( FileInputStream fis = new FileInputStream( f ); ServletOutputStream os = ctx.res.getOutputStream() ) {
                IOUtils.copyLarge( fis, os );
                os.flush();
            } catch ( IOException ignored ) {
                ctx.status( 500 );
            }
        }
        ctx.result( "" );
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
    private Result executeSqlSelect( final Statement statement, final UIRequest request, final String sqlSelect ) throws QueryExecutionException {
        return executeSqlSelect( statement, request, sqlSelect, false );
    }


    private Result executeSqlSelect( final Statement statement, final UIRequest request, final String sqlSelect, final boolean noLimit ) throws QueryExecutionException {
        PolyImplementation result;
        List<List<Object>> rows;
        boolean hasMoreRows;
        boolean isAnalyze = statement.getTransaction().isAnalyze();

        try {
            result = processQuery( statement, sqlSelect, isAnalyze );
            rows = result.getRows( statement, noLimit ? -1 : getPageSize(), true, isAnalyze );
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

        EntityType entityType = null;
        CatalogTable catalogTable = null;
        if ( request.tableId != null ) {
            String[] t = request.tableId.split( "\\." );
            try {
                catalogTable = catalog.getTable( this.databaseId, t[0], t[1] );
                entityType = catalogTable.entityType;
            } catch ( UnknownTableException e ) {
                log.error( "Caught exception", e );
            }
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

            DbColumn dbCol = new DbColumn(
                    metaData.getName(),
                    metaData.getType().getPolyType().getTypeName(),
                    metaData.getType().isNullable() == (ResultSetMetaData.columnNullable == 1),
                    metaData.getType().getPrecision(),
                    sort,
                    filter );

            // Get column default values
            if ( catalogTable != null ) {
                try {
                    if ( catalog.checkIfExistsColumn( catalogTable.id, columnName ) ) {
                        CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                        if ( catalogColumn.defaultValue != null ) {
                            dbCol.defaultValue = catalogColumn.defaultValue.value;
                        }
                    }
                } catch ( UnknownColumnException e ) {
                    log.error( "Caught exception", e );
                }
            }
            header.add( dbCol );
        }

        ArrayList<String[]> data = computeResultData( rows, header, statement.getTransaction() );

        if ( entityType != null ) {
            return new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ), result.getNamespaceType(), QueryLanguage.SQL ).setAffectedRows( data.size() ).setHasMoreRows( hasMoreRows );
        } else {
            //if we do not have a fix table it is not possible to change anything within the resultSet therefore we use EntityType.SOURCE
            return new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ), result.getNamespaceType(), QueryLanguage.SQL ).setAffectedRows( data.size() ).setHasMoreRows( hasMoreRows );
        }
    }


    /**
     * Convert data from a query result to Strings readable in the UI
     *
     * @param rows Rows from the enumerable iterator
     * @param header Header from the UI-ResultSet
     */
    public static ArrayList<String[]> computeResultData( final List<List<Object>> rows, final List<DbColumn> header, final Transaction transaction ) {
        ArrayList<String[]> data = new ArrayList<>();
        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    switch ( header.get( counter ).dataType ) {
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
                        case "SOUND":
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
                                    if ( f.isDirectory() && transaction.getInvolvedAdapters().stream().anyMatch( a -> a.getAdapterName().equals( "QFS" ) ) ) {
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
                    }
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


    private PolyImplementation processQuery( Statement statement, String sql, boolean isAnalyze ) {
        PolyImplementation result;
        if ( isAnalyze ) {
            statement.getOverviewDuration().start( "Parsing" );
        }
        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );
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
        PolyImplementation result;

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
        Result result = executeSqlSelect( statement, request, query );
        // We expect the result to be in the first column of the first row
        if ( result.getData().length == 0 ) {
            return 0;
        } else {
            if ( statement.getMonitoringEvent() != null ) {
                StatementEvent eventData = statement.getMonitoringEvent();
                eventData.setRowCount( Integer.parseInt( result.getData()[0][0] ) );
            }
            return Integer.parseInt( result.getData()[0][0] );
        }
    }


    /**
     * Get the number of rows that should be displayed in one page in the data view
     */
    private int getPageSize() {
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }


    private boolean isClassificationToSql() {
        return RuntimeConfig.EXPLORE_BY_EXAMPLE_TO_SQL.getBoolean();
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
        return getTransaction( false, true );
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, TransactionManager transactionManager, long userId, long databaseId ) {
        return getTransaction( analyze, useCache, transactionManager, userId, databaseId, "Polypheny-UI" );
    }


    public static Transaction getTransaction( boolean analyze, boolean useCache, TransactionManager transactionManager, long userId, long databaseId, String origin ) {
        try {
            Transaction transaction = transactionManager.startTransaction( userId, databaseId, analyze, origin, MultimediaFlavor.FILE );
            transaction.setUseCache( useCache );
            return transaction;
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    public Transaction getTransaction( boolean analyze, boolean useCache ) {
        return getTransaction( analyze, useCache, transactionManager, userId, databaseId );
    }


    /**
     * Get the data types of each column of a table
     *
     * @param schemaName name of the schema
     * @param tableName name of the table
     * @return HashMap containing the type of each column. The key is the name of the column and the value is the Sql ExpressionType (java.sql.Types).
     */
    private Map<String, CatalogColumn> getCatalogColumns( String schemaName, String tableName ) {
        Map<String, CatalogColumn> dataTypes = new HashMap<>();
        try {
            CatalogTable table = catalog.getTable( this.databaseId, schemaName, tableName );
            List<CatalogColumn> catalogColumns = catalog.getColumns( table.id );
            for ( CatalogColumn catalogColumn : catalogColumns ) {
                dataTypes.put( catalogColumn.name, catalogColumn );
            }
        } catch ( UnknownTableException e ) {
            log.error( "Caught exception", e );
        }
        return dataTypes;
    }


    /**
     * Helper function to delete a directory.
     * Taken from https://www.baeldung.com/java-delete-directory
     */
    boolean deleteDirectory( final File directoryToBeDeleted ) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if ( allContents != null ) {
            for ( File file : allContents ) {
                deleteDirectory( file );
            }
        }
        return directoryToBeDeleted.delete();
    }


    void getTypeSchemas( final Context ctx ) {
        ctx.json( catalog
                .getSchemas( 1, null )
                .stream()
                .collect( Collectors.toMap( CatalogSchema::getName, CatalogSchema::getNamespaceType ) ) );
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
