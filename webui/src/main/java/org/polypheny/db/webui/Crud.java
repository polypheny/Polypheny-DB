/*
 * Copyright 2019-2020 The Polypheny Project
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
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.SqlProcessor;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.Store.AdapterSetting;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.adapter.StoreManager.AdapterInformation;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.NameGenerator;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationHtml;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.parser.SqlParser;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.statistic.StatisticsManager;
import org.polypheny.db.statistic.exploreByExample.Explore;
import org.polypheny.db.statistic.exploreByExample.ExploreManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.webui.SchemaToJsonMapper.JsonColumn;
import org.polypheny.db.webui.SchemaToJsonMapper.JsonTable;
import org.polypheny.db.webui.models.Adapter;
import org.polypheny.db.webui.models.DbColumn;
import org.polypheny.db.webui.models.DbTable;
import org.polypheny.db.webui.models.Debug;
import org.polypheny.db.webui.models.ExploreResult;
import org.polypheny.db.webui.models.ForeignKey;
import org.polypheny.db.webui.models.HubResult;
import org.polypheny.db.webui.models.Index;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.ResultType;
import org.polypheny.db.webui.models.Schema;
import org.polypheny.db.webui.models.SidebarElement;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.Status;
import org.polypheny.db.webui.models.TableConstraint;
import org.polypheny.db.webui.models.UIRelNode;
import org.polypheny.db.webui.models.Uml;
import org.polypheny.db.webui.models.requests.ColumnRequest;
import org.polypheny.db.webui.models.requests.ConstraintRequest;
import org.polypheny.db.webui.models.requests.EditTableRequest;
import org.polypheny.db.webui.models.requests.ClassifyAllData;
import org.polypheny.db.webui.models.requests.ExploreData;
import org.polypheny.db.webui.models.requests.ExploreTables;
import org.polypheny.db.webui.models.requests.HubRequest;
import org.polypheny.db.webui.models.requests.QueryExplorationRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.SchemaTreeRequest;
import org.polypheny.db.webui.models.requests.UIRequest;
import spark.Request;
import spark.Response;
import spark.utils.IOUtils;


@Slf4j
public class Crud implements InformationObserver {

    private final Gson gson = new Gson();
    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;
    private final StatisticsManager store = StatisticsManager.getInstance();
    private boolean isActiveTracking = false;
    private final Catalog catalog = Catalog.getInstance();


    /**
     * Constructor
     *
     * @param transactionManager The Polypheny-DB transaction manager
     */
    Crud( final TransactionManager transactionManager, final String userName, final String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;
        registerStatisticObserver();
    }


    /**
     * Ensures that changes in the ConfigManger toggle the statistics correctly
     */
    private void registerStatisticObserver() {
        this.isActiveTracking = RuntimeConfig.ACTIVE_TRACKING.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean();
        ConfigListener observer = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                setConfig( c );
            }


            @Override
            public void restart( Config c ) {
                setConfig( c );
            }


            private void setConfig( Config c ) {
                isActiveTracking = c.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean();
            }
        };
        RuntimeConfig.ACTIVE_TRACKING.addObserver( observer );
        RuntimeConfig.DYNAMIC_QUERYING.addObserver( observer );
    }


    /**
     * Returns the content of a table with a maximum of PAGESIZE elements.
     */
    Result getTable( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
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
                .append( orderBy )
                .append( " LIMIT " )
                .append( getPageSize() )
                .append( " OFFSET " )
                .append( (Math.max( 0, request.currentPage - 1 )) * getPageSize() );

        try {
            result = executeSqlSelect( transaction, request, query.toString() );
        } catch ( QueryExecutionException e ) {
            //result = new Result( e.getMessage() );
            log.error( "Caught exception while fetching a table", e );
            result = new Result( "Could not fetch table " + request.tableId );
            try {
                transaction.rollback();
                return result;
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        // determine if it is a view or a table
        try {
            CatalogTable catalogTable = catalog.getTable( this.databaseName, t[0], t[1] );
            if ( catalogTable.tableType == TableType.TABLE ) {
                result.setType( ResultType.TABLE );
            } else if ( catalogTable.tableType == TableType.VIEW ) {
                result.setType( ResultType.VIEW );
            } else {
                throw new RuntimeException( "Unknown table type: " + catalogTable.tableType );
            }
        } catch ( GenericCatalogException | UnknownTableException e ) {
            log.error( "Caught exception", e );
            result.setError( "Could not retrieve type of Result (table/view)." );
        }

        result.setCurrentPage( request.currentPage ).setTable( request.tableId );
        int tableSize = 0;
        try {
            tableSize = getTableSize( transaction, request );
        } catch ( QueryExecutionException e ) {
            log.error( "Caught exception while determining page size", e );
        }
        result.setHighestPage( (int) Math.ceil( (double) tableSize / getPageSize() ) );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while committing transaction", e );
        }
        return result;
    }


    ArrayList<SidebarElement> getSchemaTree( final Request req, final Response res ) {
        SchemaTreeRequest request = this.gson.fromJson( req.body(), SchemaTreeRequest.class );
        ArrayList<SidebarElement> result = new ArrayList<>();

        if ( request.depth < 1 ) {
            log.error( "Trying to fetch a schemaTree with depth < 1" );
            return new ArrayList<>();
        }

        try {
            List<CatalogSchema> schemas = catalog.getSchemas( new Catalog.Pattern( databaseName ), null );
            for ( CatalogSchema schema : schemas ) {
                SidebarElement schemaTree = new SidebarElement( schema.name, schema.name, "", "cui-layers" );

                if ( request.depth > 1 ) {
                    ArrayList<SidebarElement> tables = new ArrayList<>();
                    ArrayList<SidebarElement> views = new ArrayList<>();
                    List<CatalogTable> childTables = catalog.getTables( schema.id, null );
                    for ( CatalogTable childTable : childTables ) {
                        SidebarElement table = new SidebarElement( childTable.schemaName + "." + childTable.name, childTable.name, request.routerLinkRoot, "fa fa-table" );

                        if ( request.depth > 2 ) {
                            List<CatalogColumn> childColumns = catalog.getColumns( childTable.id );
                            for ( CatalogColumn childColumn : childColumns ) {
                                table.addChild( new SidebarElement( childColumn.schemaName + "." + childColumn.tableName + "." + childColumn.name, childColumn.name, request.routerLinkRoot ).setCssClass( "sidebarColumn" ) );
                            }
                        }
                        if ( childTable.tableType == TableType.TABLE ) {
                            tables.add( table );
                        } else if ( request.views && childTable.tableType == TableType.VIEW ) {
                            views.add( table );
                        }
                    }
                    if( request.showTable ) {
                        schemaTree.addChild( new SidebarElement( schema.name + ".tables", "tables", request.routerLinkRoot, "fa fa-table" ).addChildren( tables ).setRouterLink( "" ) );
                    }else {
                        schemaTree.addChildren( tables );
                    }
                    if ( request.views ) {
                        schemaTree.addChild( new SidebarElement( schema.name + ".views", "views", request.routerLinkRoot, "icon-eye" ).addChildren( views ).setRouterLink( "" ) );
                    }
                }
                result.add( schemaTree );
            }
        } catch ( GenericCatalogException | UnknownSchemaException e ) {
            log.error( "Caught exception", e );
        }

        return result;
    }


    /**
     * Get all tables of a schema
     */
    Result getTables( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );

        Result result;
        try {
            List<CatalogTable> tables = catalog.getTables( new Catalog.Pattern( databaseName ), new Catalog.Pattern( request.schema ), null );
            ArrayList<String> tableNames = new ArrayList<>();
            for ( CatalogTable catalogTable : tables ) {
                tableNames.add( catalogTable.name );
            }
            result = new Result( new Debug().setAffectedRows( tableNames.size() ) ).setTables( tableNames );
        } catch ( GenericCatalogException e ) {
            log.error( "Caught exception while fetching tables", e );
            result = new Result( e );
        }
        return result;
    }


    /**
     * Drop or truncate a table
     */
    Result dropTruncateTable( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        StringBuilder query = new StringBuilder();
        if ( request.action.toLowerCase().equals( "drop" ) ) {
            query.append( "DROP TABLE " );
        } else if ( request.action.toLowerCase().equals( "truncate" ) ) {
            query.append( "TRUNCATE TABLE " );
        }
        String tableId = String.format( "\"%s\".\"%s\"", request.schema, request.table );
        query.append( tableId );
        try {
            int a = executeSqlUpdate( transaction, query.toString() );
            result = new Result( new Debug().setAffectedRows( a ).setGeneratedQuery( query.toString() ) );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping or truncating a table", e );
            result = new Result( e ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Create a new table
     */
    Result createTable( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
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
            if ( col.maxLength != null ) {
                colBuilder.append( String.format( "(%d)", col.maxLength ) );
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
                primaryKeys.add( col.name );
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
            result = new Result( new Debug().setGeneratedQuery( query.toString() ).setAffectedRows( a ) );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating a table", e );
            result = new Result( e ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback CREATE TABLE statement: {}", ex.getMessage(), ex );
            }
        }
        return result;
    }


    /**
     * Insert data into a table
     */
    Result insertRow( final Request req, final Response res ) {
        Transaction transaction = getTransaction();
        int rowsAffected;
        Result result;
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        StringJoiner cols = new StringJoiner( ",", "(", ")" );
        StringBuilder query = new StringBuilder();
        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        query.append( "INSERT INTO " ).append( tableId );
        StringJoiner values = new StringJoiner( ",", "(", ")" );

        Map<String, PolyType> dataTypes = getColumnTypes( t[0], t[1] );
        for ( Map.Entry<String, String> entry : request.data.entrySet() ) {
            cols.add( "\"" + entry.getKey() + "\"" );
            String value = entry.getValue();
            if ( value == null ) {
                value = "NULL";
            } else if ( dataTypes.get( entry.getKey() ).getFamily() == PolyTypeFamily.CHARACTER ) {
                value = "'" + StringEscapeUtils.escapeSql( value ) + "'";
            } else if ( dataTypes.get( entry.getKey() ) == PolyType.DATE ) {
                value = "DATE '" + value + "'";
            } else if ( dataTypes.get( entry.getKey() ) == PolyType.TIME ) {
                value = "TIME '" + value + "'";
            } else if ( dataTypes.get( entry.getKey() ) == PolyType.TIMESTAMP ) {
                value = "TIMESTAMP '" + value + "'";
            }
            values.add( value );
        }

        if ( isActiveTracking ) {
            transaction.addChangedTable( tableId );
        }

        query.append( cols.toString() );
        query.append( " VALUES " ).append( values.toString() );

        try {
            rowsAffected = executeSqlUpdate( transaction, query.toString() );
            result = new Result( new Debug().setAffectedRows( rowsAffected ).setGeneratedQuery( query.toString() ) );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
            log.error( "Caught exception while inserting a row", e );
            result = new Result( e ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Run any query coming from the SQL console
     */
    ArrayList<Result> anyQuery( final Request req, final Response res ) {
        QueryRequest request = this.gson.fromJson( req.body(), QueryRequest.class );
        Transaction transaction = getTransaction( request.analyze );

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
        String[] queries = allQueries.split( ";", 0 );
        boolean noLimit = false;
        for ( String query : queries ) {
            Result result;
            if ( Pattern.matches( "(?si:[\\s]*COMMIT.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    transaction.commit();
                    executionTime += System.nanoTime() - temp;
                    transaction = getTransaction( request.analyze );
                    results.add( new Result( new Debug().setGeneratedQuery( query ) ) );
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
                    transaction = getTransaction( request.analyze );
                    results.add( new Result( new Debug().setGeneratedQuery( query ) ) );
                } catch ( TransactionException e ) {
                    log.error( "Caught exception while rolling back a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                }
            } else if ( Pattern.matches( "(?si:^[\\s]*SELECT.*)", query ) ) {
                // Add limit if not specified
                Pattern p2 = Pattern.compile( ".*?(?si:limit)[\\s\\S]*", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
                if ( !p2.matcher( query ).find() ) {
                    query = query + " LIMIT " + getPageSize();
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
                    result = executeSqlSelect( transaction, request, query, noLimit ).setInfo( new Debug().setGeneratedQuery( query ) );
                    executionTime += System.nanoTime() - temp;
                    results.add( result );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e ).setInfo( new Debug().setGeneratedQuery( query ) );
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
                    result = new Result( new Debug().setAffectedRows( numOfRows ).setGeneratedQuery( query ) );
                    results.add( result );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze );
                    }
                } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                    try {
                        transaction.rollback();
                    } catch ( TransactionException ex ) {
                        log.error( "Caught exception while rollback", e );
                    }
                }
            }

        }

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception", e );
            results.add( new Result( e ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }

        if ( queryAnalyzer != null ) {
            InformationPage p1 = new InformationPage( "Query analysis", "Analysis of the query." );
            InformationGroup g1 = new InformationGroup( p1, "Execution time" );
            InformationHtml html;
            if ( executionTime < 1e4 ) {
                html = new InformationHtml( g1, String.format( "Execution time: %d nanoseconds", executionTime ) );
            } else {
                long millis = TimeUnit.MILLISECONDS.convert( executionTime, TimeUnit.NANOSECONDS );
                // format time: see: https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java#answer-625444
                DateFormat df = new SimpleDateFormat( "m 'min' s 'sec' S 'ms'" );
                String durationText = df.format( new Date( millis ) );
                html = new InformationHtml( g1, String.format( "Execution time: %s", durationText ) );
            }
            queryAnalyzer.addPage( p1 );
            queryAnalyzer.addGroup( g1 );
            queryAnalyzer.registerInformation( html );
        }

        return results;
    }


    /**
     * Return all available statistics to the client
     */
    ConcurrentHashMap<?, ?> getStatistics( final Request req, final Response res ) {
        if ( RuntimeConfig.DYNAMIC_QUERYING.getBoolean() ) {
            return store.getStatisticSchemaMap();
        } else {
            return new ConcurrentHashMap<>();
        }

    }


    /**
     * Gets the classified Data from User
     * return possibly interesting Data to User
     */
    public Result classifyData( Request req, Response res ) {
        ClassifyAllData classifyAllData = this.gson.fromJson( req.body(), ClassifyAllData.class );
        ExploreManager exploreManager = ExploreManager.getInstance();

        boolean isConvertedToSql = isClassificationToSql();

        Explore explore = exploreManager.classifyData( classifyAllData.id, classifyAllData.classified, isConvertedToSql );

        if ( isConvertedToSql ) {
            Transaction transaction = getTransaction();
            Result result;

            try {
                result = executeSqlSelect( transaction, classifyAllData, explore.getClassifiedSqlStatement(), false ).setInfo( new Debug().setGeneratedQuery( explore.getClassifiedSqlStatement() ) );
                transaction.commit();
                transaction = getTransaction( classifyAllData.analyze );

            } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                log.error( "Caught exception while executing a query from the console", e );
                result = new Result( e ).setInfo( new Debug().setGeneratedQuery( explore.getClassifiedSqlStatement() ) );
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

            return result;
        } else {
            Result result = new Result( classifyAllData.header, Arrays.copyOfRange( explore.getData(), 0, 10 ) );

            result.setClassificationInfo( "NoClassificationPossible" );
            result.setExplorerId( explore.getId() );

            result.setCurrentPage( classifyAllData.cPage ).setTable( classifyAllData.tableId );
            result.setHighestPage( (int) Math.ceil( (double) explore.getData().length / getPageSize() ) );
            result.setConvertedToSql( isConvertedToSql );
            return result;
        }

    }


    /**
     * For pagination within the Explore-by-Example table
     */
    public Object getExploreTables( Request request, Response response ) {

        ExploreTables exploreTables = this.gson.fromJson( request.body(), ExploreTables.class );
        Transaction transaction = getTransaction();
        Result result;
        ExploreManager exploreManager = ExploreManager.getInstance();
        Explore explore = exploreManager.getExploreInformation( exploreTables.id );
        String[][] paginationData;

        String query = explore.getSqlStatment() + " OFFSET " + ((Math.max( 0, exploreTables.cPage - 1 )) * getPageSize());

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

            return result;
        }

        try {
            result = executeSqlSelect( transaction, exploreTables, query );
        } catch ( QueryExecutionException e ) {
            log.error( "Caught exception while fetching a table", e );
            result = new Result( "Could not fetch table " + exploreTables.tableId );
            try {
                transaction.rollback();
                return result;
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
        int tableSize = 0;
        tableSize = explore.getTableSize();

        result.setHighestPage( (int) Math.ceil( (double) tableSize / getPageSize() ) );

        if ( !explore.isClassificationPossible() ) {
            result.setClassificationInfo( "NoClassificationPossible" );
        } else {
            result.setClassificationInfo( "ClassificationPossible" );
        }
        result.setIncludesClassificationInfo( explore.isDataAfterClassification );

        if ( explore.isDataAfterClassification ) {
            int tablesize = explore.getDataAfterClassification().size();
            List<String[]> paginationDataList = new ArrayList<>();
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
        return result;

    }


    /**
     * Creates the initial query for the Explore-by-Example process
     */
    public Result createInitialExploreQuery( Request req, Response res ) {

        QueryExplorationRequest queryExplorationRequest = this.gson.fromJson( req.body(), QueryExplorationRequest.class );
        ExploreManager exploreManager = ExploreManager.getInstance();
        Transaction transaction = getTransaction( queryExplorationRequest.analyze );

        Result result;

        Explore explore = exploreManager.createSqlQuery( null, queryExplorationRequest.query );
        if ( explore.getDataType() == null ) {
            return new Result( "Explore by Example is only available for tables with the following datatypes: VARCHAR, INTEGER, SMALLINT, TINYINT, BIGINT, DECIMAL" );
        }

        String query = explore.getSqlStatment();
        try {
            result = executeSqlSelect( transaction, queryExplorationRequest, query, false ).setInfo( new Debug().setGeneratedQuery( query ) );
            transaction.commit();
            transaction = getTransaction( queryExplorationRequest.analyze );

        } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            result = new Result( e ).setInfo( new Debug().setGeneratedQuery( query ) );
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

        return result;
    }


    /**
     * Start Classification, classifies the initial dataset, to show what would be within the final result set
     */
    public ExploreResult exploration( Request req, Response res ) {
        ExploreData exploreData = this.gson.fromJson( req.body(), ExploreData.class );

        String[] dataType = new String[exploreData.header.length + 1];
        for ( int i = 0; i < exploreData.header.length; i++ ) {
            dataType[i] = exploreData.header[i].dataType;
        }
        dataType[exploreData.header.length] = "VARCHAR";

        ExploreManager e = ExploreManager.getInstance();
        Explore explore = e.exploreData( exploreData.id, exploreData.classified, dataType );

        return new ExploreResult( exploreData.header, explore.getDataAfterClassification(), explore.getId(), explore.getBuildGraph() );
    }


    /**
     * Delete a row from a table. The row is determined by the value of every column in that row (conjunction).
     * The transaction is being rolled back, if more that one row would be deleted.
     * TODO: This is not a nice solution
     */
    Result deleteRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        StringBuilder builder = new StringBuilder();
        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        builder.append( "DELETE FROM " ).append( tableId ).append( " WHERE " );
        StringJoiner joiner = new StringJoiner( " AND ", "", "" );
        Map<String, PolyType> dataTypes = getColumnTypes( t[0], t[1] );
        String column = "";
        for ( Entry<String, String> entry : request.data.entrySet() ) {
            String condition;
            if ( entry.getValue() == null ) {
                condition = String.format( "\"%s\" IS NULL", entry.getKey() );
            } else if ( dataTypes.get( entry.getKey() ).getFamily() != PolyTypeFamily.CHARACTER ) {
                condition = String.format( "\"%s\" = %s", entry.getKey(), entry.getValue() );
            } else {
                condition = String.format( "\"%s\" = '%s'", entry.getKey(), StringEscapeUtils.escapeSql( entry.getValue() ) );
            }
            column = entry.getKey();
            joiner.add( condition );
        }
        builder.append( joiner.toString() );

        try {
            int numOfRows = executeSqlUpdate( transaction, builder.toString() );
            // only commit if one row is deleted
            if ( numOfRows == 1 ) {
                if ( isActiveTracking ) {
                    transaction.addChangedTable( tableId );
                }

                transaction.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                transaction.rollback();
                result = new Result( "Attempt to delete " + numOfRows + " rows was blocked." );
                result.setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while deleting a row", e );
            result = new Result( e ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    Result updateRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        StringBuilder builder = new StringBuilder();
        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        builder.append( "UPDATE " ).append( tableId ).append( " SET " );
        StringJoiner setStatements = new StringJoiner( ",", "", "" );
        String column = "";
        for ( Entry<String, String> entry : request.data.entrySet() ) {
            if ( entry.getValue() == null ) {
                setStatements.add( String.format( "\"%s\" = NULL", entry.getKey() ) );
            } else if ( NumberUtils.isNumber( entry.getValue() ) ) {
                setStatements.add( String.format( "\"%s\" = %s", entry.getKey(), entry.getValue() ) );
            } else {
                setStatements.add( String.format( "\"%s\" = '%s'", entry.getKey(), StringEscapeUtils.escapeSql( entry.getValue() ) ) );
            }
            column = entry.getKey();
        }
        builder.append( setStatements.toString() );

        StringJoiner where = new StringJoiner( " AND ", "", "" );
        for ( Entry<String, String> entry : request.filter.entrySet() ) {
            where.add( String.format( "\"%s\" = '%s'", entry.getKey(), entry.getValue() ) );
        }
        builder.append( " WHERE " ).append( where.toString() );

        try {
            int numOfRows = executeSqlUpdate( transaction, builder.toString() );

            if ( numOfRows == 1 ) {
                if ( isActiveTracking ) {
                    transaction.addChangedTable( tableId );
                }
                transaction.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                transaction.rollback();
                result = new Result( "Attempt to update " + numOfRows + " rows was blocked." );
                result.setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a row", e );
            result = new Result( e ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Get the columns of a table
     */
    Result getColumns( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;

        String[] t = request.tableId.split( "\\." );
        ArrayList<DbColumn> cols = new ArrayList<>();

        try {
            CatalogTable catalogTable = catalog.getTable( databaseName, t[0], t[1] );
            ArrayList<String> primaryColumns;
            if ( catalogTable.primaryKey != null ) {
                CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                primaryColumns = new ArrayList<>( primaryKey.columnNames );
            } else {
                primaryColumns = new ArrayList<>();
            }
            for ( CatalogColumn catalogColumn : catalog.getColumns( catalogTable.id ) ) {
                String defaultValue = catalogColumn.defaultValue == null ? null : catalogColumn.defaultValue.value;
                cols.add(
                        new DbColumn(
                                catalogColumn.name,
                                catalogColumn.type.name(),
                                catalogColumn.nullable,
                                catalogColumn.length,
                                primaryColumns.contains( catalogColumn.name ),
                                defaultValue ) );
            }
            result = new Result( cols.toArray( new DbColumn[0] ), null );
        } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException e ) {
            log.error( "Caught exception while getting a column", e );
            result = new Result( e );
        }

        return result;
    }


    Result updateColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
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
            String query = String.format( "ALTER TABLE %s RENAME COLUMN \"%s\" TO \"%s\"", tableId, oldColumn.name, newColumn.name );
            queries.add( query );
        }

        // change type + length
        // TODO: cast if needed
        if ( !oldColumn.dataType.equals( newColumn.dataType ) || !Objects.equals( oldColumn.maxLength, newColumn.maxLength ) ) {
            if ( newColumn.maxLength != null ) {
                String query = String.format( "ALTER TABLE %s MODIFY COLUMN \"%s\" SET TYPE %s(%s)", tableId, newColumn.name, newColumn.dataType, newColumn.maxLength );
                queries.add( query );
            } else {
                // TODO: drop maxlength if requested
                String query = String.format( "ALTER TABLE %s MODIFY COLUMN \"%s\" SET TYPE %s", tableId, newColumn.name, newColumn.dataType );
                queries.add( query );
            }
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
            queries.add( query );
        }

        result = new Result( new Debug().setAffectedRows( 1 ).setGeneratedQuery( queries.toString() ) );
        try {
            for ( String query : queries ) {
                executeSqlUpdate( transaction, query );
                sBuilder.append( query );
            }
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a column", e );
            result = new Result( e ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught exception during rollback", e2 );
                result = new Result( e2 ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() ) );
            }
        }

        return result;
    }


    /**
     * Add a column to an existing table
     */
    Result addColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        Transaction transaction = getTransaction();

        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        String query = String.format( "ALTER TABLE %s ADD COLUMN \"%s\" %s", tableId, request.newColumn.name, request.newColumn.dataType );
        if ( request.newColumn.maxLength != null ) {
            query = query + String.format( "(%d)", request.newColumn.maxLength );
        }
        if ( !request.newColumn.nullable ) {
            query = query + " NOT NULL";
        }
        if ( request.newColumn.defaultValue != null ) {
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
                    query = query + " DEFAULT " + b.toString();
                    break;
                case "VARCHAR":
                    query = query + String.format( " DEFAULT '%s'", request.newColumn.defaultValue );
                    break;
                default:
                    query = query + " DEFAULT " + request.newColumn.defaultValue;
            }
        }
        Result result;
        try {
            int affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( affectedRows ).setGeneratedQuery( query ) );
        } catch ( TransactionException | QueryExecutionException e ) {
            log.error( "Caught exception while adding a column", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Delete a column of a table
     */
    Result dropColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        Transaction transaction = getTransaction();

        String[] t = request.tableId.split( "\\." );
        String tableId = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result result;
        String query = String.format( "ALTER TABLE %s DROP COLUMN \"%s\"", tableId, request.oldColumn.name );
        try {
            int affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( affectedRows ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a column", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Get artificially generated index/foreign key/constraint names for placeholders in the UI
     */
    Result getGeneratedNames( final Request req, final Response res ) {
        String[] data = new String[3];
        data[0] = NameGenerator.generateConstraintName();
        data[1] = NameGenerator.generateForeignKeyName();
        data[2] = NameGenerator.generateIndexName();
        return new Result( new DbColumn[0], new String[][]{ data } );
    }


    /**
     * Get constraints of a table
     */
    Result getConstraints( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;

        String[] t = request.tableId.split( "\\." );
        ArrayList<TableConstraint> resultList = new ArrayList<>();
        Map<String, ArrayList<String>> temp = new HashMap<>();

        try {
            CatalogTable catalogTable = catalog.getTable( databaseName, t[0], t[1] );

            // get primary key
            if ( catalogTable.primaryKey != null ) {
                CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                // TODO: This does not really make much sense... A table can only have one primary key at the same time.
                for ( String columnName : primaryKey.columnNames ) {
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
                    temp.put( catalogConstraint.name, new ArrayList<>( catalogConstraint.key.columnNames ) );
                }
            }
            for ( Map.Entry<String, ArrayList<String>> entry : temp.entrySet() ) {
                resultList.add( new TableConstraint( entry.getKey(), "UNIQUE", entry.getValue() ) );
            }

            // get foreign keys
            temp.clear();
            List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( catalogTable.id );
            for ( CatalogForeignKey catalogForeignKey : foreignKeys ) {
                temp.put( catalogForeignKey.name, new ArrayList<>( catalogForeignKey.columnNames ) );
            }
            for ( Map.Entry<String, ArrayList<String>> entry : temp.entrySet() ) {
                resultList.add( new TableConstraint( entry.getKey(), "FOREIGN KEY", entry.getValue() ) );
            }

            DbColumn[] header = { new DbColumn( "Constraint name" ), new DbColumn( "Constraint type" ), new DbColumn( "Columns" ) };
            ArrayList<String[]> data = new ArrayList<>();
            resultList.forEach( c -> data.add( c.asRow() ) );

            result = new Result( header, data.toArray( new String[0][2] ) );
        } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException e ) {
            log.error( "Caught exception while fetching constraints", e );
            result = new Result( e );
        }

        return result;
    }


    Result dropConstraint( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
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
            result = new Result( new Debug().setAffectedRows( rows ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a constraint", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Add a primary key to a table
     */
    Result addPrimaryKey( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
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
                result = new Result( new Debug().setAffectedRows( rows ).setGeneratedQuery( query ) );
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
        return result;
    }


    /**
     * Add a primary key to a table
     */
    Result addUniqueConstraint( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
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
                result = new Result( new Debug().setAffectedRows( rows ).setGeneratedQuery( query ) );
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
        return result;
    }


    /**
     * Get indexes of a table
     */
    Result getIndexes( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Result result;
        try {
            CatalogTable catalogTable = catalog.getTable( databaseName, request.schema, request.table );
            List<CatalogIndex> catalogIndexes = catalog.getIndexes( catalogTable.id, false );

            DbColumn[] header = { new DbColumn( "name" ), new DbColumn( "columns" ), new DbColumn( "type" ) };

            ArrayList<String[]> data = new ArrayList<>();
            for ( CatalogIndex catalogIndex : catalogIndexes ) {
                String[] arr = new String[3];
                arr[0] = catalogIndex.name;
                arr[1] = String.join( ", ", catalogIndex.key.columnNames );
                arr[2] = catalogIndex.type.name();
                data.add( arr );
            }

            result = new Result( header, data.toArray( new String[0][2] ) );

        } catch ( UnknownTableException | GenericCatalogException e ) {
            log.error( "Caught exception while fetching indexes", e );
            result = new Result( e );
        }
        return result;
    }


    /**
     * Drop an index of a table
     */
    Result dropIndex( final Request req, final Response res ) {
        Index index = gson.fromJson( req.body(), Index.class );
        Transaction transaction = getTransaction();

        String tableId = String.format( "\"%s\".\"%s\"", index.getSchema(), index.getTable() );
        String query = String.format( "ALTER TABLE %s DROP INDEX \"%s\"", tableId, index.getName() );
        Result result;
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setGeneratedQuery( query ).setAffectedRows( a ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping an index", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Create an index for a table
     */
    Result createIndex( final Request req, final Response res ) {
        Index index = this.gson.fromJson( req.body(), Index.class );
        Transaction transaction = getTransaction();

        String tableId = String.format( "\"%s\".\"%s\"", index.getSchema(), index.getTable() );
        Result result;
        StringJoiner colJoiner = new StringJoiner( ",", "(", ")" );
        for ( String col : index.getColumns() ) {
            colJoiner.add( "\"" + col + "\"" );
        }
        String query = String.format( "ALTER TABLE %s ADD INDEX \"%s\" ON %s USING %s", tableId, index.getName(), colJoiner.toString(), index.getMethod() );
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( a ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating an index", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Get placements of a table
     */
    Result getPlacements( final Request req, final Response res ) {
        Index index = gson.fromJson( req.body(), Index.class );
        String schemaName = index.getSchema();
        String tableName = index.getTable();
        Result result;
        try {
            CatalogTable table = catalog.getTable( databaseName, schemaName, tableName );
            // Map<Integer, List<CatalogColumnPlacement>> placementsByStore = table.placementsByStore;
            DbColumn[] header = {
                    new DbColumn( "Store" ),
                    new DbColumn( "Adapter" ),
                    new DbColumn( "DataReadOnly" ),
                    new DbColumn( "SchemaReadOnly" ),
                    new DbColumn( "Columns" ) };

            ArrayList<String[]> data = new ArrayList<>();
            for ( CatalogStore catalogStore : catalog.getStores() ) {
                Store store = StoreManager.getInstance().getStore( catalogStore.id );
                List<CatalogColumnPlacement> placements = catalog.getColumnPlacementsOnStore( catalogStore.id, table.id );
                if ( placements.size() == 0 ) {
                    continue;
                }
                String[] arr = new String[5];
                arr[0] = store.getUniqueName();
                arr[1] = store.getAdapterName();
                arr[2] = String.valueOf( store.isDataReadOnly() );
                arr[3] = String.valueOf( store.isSchemaReadOnly() );
                arr[4] = "";
                boolean first = true;
                for ( CatalogColumnPlacement p : placements ) {
                    String prefix = ", ";
                    String suffix = "";
                    if ( first ) {
                        first = false;
                        prefix = "";
                    }
                    if ( p.placementType == PlacementType.MANUAL ) {
                        prefix += "<b>";
                        suffix += "</b>";
                    }
                    arr[4] += prefix + p.columnName + suffix;
                }

                data.add( arr );
            }

            result = new Result( header, data.toArray( new String[0][4] ) );

        } catch ( GenericCatalogException | UnknownTableException e ) {
            log.error( "Caught exception while getting placements", e );
            result = new Result( e );
        }
        return result;
    }


    /**
     * Add or drop a data placement.
     * Parameter of type models.Index: index name corresponds to storeUniqueName
     * Index method: either 'ADD' or 'DROP'
     */
    Result addDropPlacement( final Request req, final Response res ) {
        Index index = gson.fromJson( req.body(), Index.class );
        if ( !index.getMethod().toUpperCase().equals( "ADD" ) && !index.getMethod().toUpperCase().equals( "DROP" ) ) {
            return new Result( "Invalid request" );
        }
        String query = String.format( "ALTER TABLE %s.%s %s PLACEMENT %s", index.getSchema(), index.getTable(), index.getMethod().toUpperCase(), index.getName() );
        Transaction transaction = getTransaction();
        int affectedRows = 0;
        try {
            affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            return new Result( e );
        }
        return new Result( new Debug().setAffectedRows( affectedRows ) );
    }


    /**
     * Get current stores
     */
    String getStores( final Request req, final Response res ) {
        //see https://futurestud.io/tutorials/gson-advanced-custom-serialization-part-1
        JsonSerializer<Store> storeSerializer = ( src, typeOfSrc, context ) -> {
            List<AdapterSetting> adapterSettings = new ArrayList<>();
            for ( AdapterSetting s : src.getAvailableSettings() ) {
                for ( String current : src.getCurrentSettings().keySet() ) {
                    if ( s.name.equals( current ) ) {
                        adapterSettings.add( s );
                    }
                }
            }

            JsonObject jsonStore = new JsonObject();
            jsonStore.addProperty( "storeId", src.getStoreId() );
            jsonStore.addProperty( "uniqueName", src.getUniqueName() );
            jsonStore.add( "adapterSettings", context.serialize( adapterSettings ) );
            jsonStore.add( "currentSettings", context.serialize( src.getCurrentSettings() ) );
            jsonStore.addProperty( "adapterName", src.getAdapterName() );
            jsonStore.addProperty( "type", src.getClass().getCanonicalName() );
            jsonStore.add( "dataReadOnly", context.serialize( src.isDataReadOnly() ) );
            jsonStore.add( "schemaReadOnly", context.serialize( src.isSchemaReadOnly() ) );
            return jsonStore;
        };
        Gson storeGson = new GsonBuilder().registerTypeAdapter( Store.class, storeSerializer ).create();
        ImmutableMap<String, Store> stores = StoreManager.getInstance().getStores();
        Store[] out = stores.values().toArray( new Store[0] );
        return storeGson.toJson( out, Store[].class );
    }


    /**
     * Update the settings of a stoe
     */
    Store updateStoreSettings( final Request req, final Response res ) {
        //see https://stackoverflow.com/questions/16872492/gson-and-abstract-superclasses-deserialization-issue
        JsonDeserializer<Store> storeDeserializer = ( json, typeOfT, context ) -> {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get( "type" ).getAsString();
            try {
                return context.deserialize( jsonObject, Class.forName( type ) );
            } catch ( ClassNotFoundException cnfe ) {
                throw new JsonParseException( "Unknown element type: " + type, cnfe );
            }
        };
        Gson storeGson = new GsonBuilder().registerTypeAdapter( Store.class, storeDeserializer ).create();
        Store store = storeGson.fromJson( req.body(), Store.class );
        StoreManager.getInstance().getStore( store.getStoreId() ).updateSettings( store.getCurrentSettings() );
        return store;
    }


    /**
     * Get available adapters
     */
    String getAdapters( final Request req, final Response res ) {
        JsonSerializer<AdapterInformation> adapterSerializer = ( src, typeOfSrc, context ) -> {
            JsonObject jsonStore = new JsonObject();
            jsonStore.addProperty( "name", src.name );
            jsonStore.addProperty( "description", src.description );
            jsonStore.addProperty( "clazz", src.clazz.getCanonicalName() );
            jsonStore.add( "adapterSettings", context.serialize( src.settings ) );
            return jsonStore;
        };
        Gson adapterGson = new GsonBuilder().registerTypeAdapter( AdapterInformation.class, adapterSerializer ).create();

        List<AdapterInformation> adapters = StoreManager.getInstance().getAvailableAdapters();
        AdapterInformation[] out = adapters.toArray( new AdapterInformation[0] );
        return adapterGson.toJson( out, AdapterInformation[].class );
    }


    /**
     * Deploy a new store
     */
    boolean addStore( final Request req, final Response res ) {
        String body = req.body();
        Adapter a = this.gson.fromJson( body, Adapter.class );

        try {
            StoreManager.getInstance().addStore( catalog, a.clazzName, a.uniqueName, a.settings );
        } catch ( Exception e ) {
            log.error( "Could not deploy store", e );
            return false;
        }
        return true;
    }


    /**
     * Remove an existing store
     */
    Result removeStore( final Request req, final Response res ) {
        String uniqueName = req.body();

        try {

            StoreManager.getInstance().removeStore( catalog, uniqueName );

        } catch ( Exception e ) {
            log.error( "Could not remove store {}", req.body(), e );
            return new Result( e );
        }
        return new Result( new Debug().setAffectedRows( 1 ) );
    }


    /**
     * Get the required information for the uml view: Foreign keys, Tables with its columns
     */
    Uml getUml( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        ArrayList<ForeignKey> fKeys = new ArrayList<>();
        ArrayList<DbTable> tables = new ArrayList<>();

        try {
            List<CatalogTable> catalogTables = catalog.getTables( new Catalog.Pattern( databaseName ), new Catalog.Pattern( request.schema ), null );
            for ( CatalogTable catalogTable : catalogTables ) {
                if ( catalogTable.tableType == TableType.TABLE ) {
                    // get foreign keys
                    List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( catalogTable.id );
                    for ( CatalogForeignKey catalogForeignKey : foreignKeys ) {
                        for ( int i = 0; i < catalogForeignKey.referencedKeyColumnNames.size(); i++ ) {
                            fKeys.add( ForeignKey.builder()
                                    .pkTableSchema( catalogForeignKey.referencedKeySchemaName )
                                    .pkTableName( catalogForeignKey.referencedKeyTableName )
                                    .pkColumnName( catalogForeignKey.referencedKeyColumnNames.get( i ) )
                                    .fkTableSchema( catalogForeignKey.schemaName )
                                    .fkTableName( catalogForeignKey.tableName )
                                    .fkColumnName( catalogForeignKey.columnNames.get( i ) )
                                    .fkName( catalogForeignKey.name )
                                    .pkName( "" ) // TODO
                                    .build() );
                        }
                    }

                    // get tables with its columns
                    DbTable table = new DbTable( catalogTable.name, catalogTable.schemaName );
                    for ( String columnName : catalogTable.columnNames ) {
                        table.addColumn( new DbColumn( columnName ) );
                    }

                    // get primary key with its columns
                    if ( catalogTable.primaryKey != null ) {
                        CatalogPrimaryKey catalogPrimaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                        for ( String columnName : catalogPrimaryKey.columnNames ) {
                            table.addPrimaryKeyField( columnName );
                        }
                    }

                    // get unique constraints
                    List<CatalogConstraint> catalogConstraints = catalog.getConstraints( catalogTable.id );
                    for ( CatalogConstraint catalogConstraint : catalogConstraints ) {
                        if ( catalogConstraint.type == ConstraintType.UNIQUE ) {
                            // TODO: unique constraints can be over multiple columns.
                            if ( catalogConstraint.key.columnNames.size() == 1 &&
                                    catalogConstraint.key.schemaName.equals( table.getSchema() ) &&
                                    catalogConstraint.key.tableName.equals( table.getTableName() ) ) {
                                table.addUniqueColumn( catalogConstraint.key.columnNames.get( 0 ) );
                            }
                            // table.addUnique( new ArrayList<>( catalogConstraint.key.columnNames ));
                        }
                    }

                    // get unique indexes
                    List<CatalogIndex> catalogIndexes = catalog.getIndexes( catalogTable.id, true );
                    for ( CatalogIndex catalogIndex : catalogIndexes ) {
                        // TODO: unique indexes can be over multiple columns.
                        if ( catalogIndex.key.columnNames.size() == 1 &&
                                catalogIndex.key.schemaName.equals( table.getSchema() ) &&
                                catalogIndex.key.tableName.equals( table.getTableName() ) ) {
                            table.addUniqueColumn( catalogIndex.key.columnNames.get( 0 ) );
                        }
                        // table.addUnique( new ArrayList<>( catalogIndex.key.columnNames ));
                    }

                    tables.add( table );
                }
            }
        } catch ( GenericCatalogException | UnknownKeyException e ) {
            log.error( "Could not fetch foreign keys of the schema {}", request.schema, e );
        }

        return new Uml( tables, fKeys );
    }


    /**
     * Add foreign key
     */
    Result addForeignKey( final Request req, final Response res ) {
        ForeignKey fk = this.gson.fromJson( req.body(), ForeignKey.class );
        Transaction transaction = getTransaction();

        String[] t = fk.getFkTableName().split( "\\." );
        String fkTable = String.format( "\"%s\".\"%s\"", t[0], t[1] );
        t = fk.getPkTableName().split( "\\." );
        String pkTable = String.format( "\"%s\".\"%s\"", t[0], t[1] );

        Result result;
        try {
            String sql = String.format( "ALTER TABLE %s ADD CONSTRAINT \"%s\" FOREIGN KEY (\"%s\") REFERENCES %s(\"%s\") ON UPDATE %s ON DELETE %s",
                    fkTable, fk.getFkName(), fk.getFkColumnName(), pkTable, fk.getPkColumnName(), fk.getUpdate(), fk.getDelete() );
            executeSqlUpdate( transaction, sql );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( 1 ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while adding a foreign key", e );
            result = new Result( e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Execute a logical plan coming from the Web-Ui plan builder
     */
    Result executeRelAlg( final Request req, final Response res ) {
        UIRelNode topNode = gson.fromJson( req.body(), UIRelNode.class );

        Transaction transaction = getTransaction( true );
        transaction.resetQueryProcessor();

        InformationManager im = transaction.getQueryAnalyzer().observe( this );
        im.addPage( new InformationPage( "Query analysis" ) );

        RelNode result;
        try {
            result = QueryPlanBuilder.buildFromTree( topNode, transaction );
        } catch ( Exception e ) {
            log.error( "Caught exception while building the plan builder tree", e );
            return new Result( e );
        }

        // Wrap RelNode into a RelRoot
        final RelDataType rowType = result.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                result instanceof Sort
                        ? ((Sort) result).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( result, result.getRowType(), SqlKind.SELECT, fields, collation );

        // Prepare
        PolyphenyDbSignature signature = transaction.getQueryProcessor().prepareQuery( root );

        List<List<Object>> rows;
        try {
            @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( transaction.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, getPageSize() ), new ArrayList<>() );
            stopWatch.stop();
            signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );
        } catch ( Exception e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            return new Result( e );
        }

        ArrayList<String[]> data = new ArrayList<>();
        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                temp[counter] = o.toString();
                counter++;
            }
            data.add( temp );
        }

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while committing the plan builder tree", e );
            throw new RuntimeException( e );
        }

        DbColumn[] header = new DbColumn[signature.columns.size()];
        int counter = 0;
        for ( ColumnMetaData col : signature.columns ) {
            header[counter++] = new DbColumn( col.columnName );
        }
        return new Result( header, data.toArray( new String[0][] ) );
    }


    /**
     * Create or drop a schema
     */
    Result schemaRequest( final Request req, final Response res ) {
        Schema schema = this.gson.fromJson( req.body(), Schema.class );
        Transaction transaction = getTransaction();

        // create schema
        if ( schema.isCreate() && !schema.isDrop() ) {
            StringBuilder query = new StringBuilder( "CREATE SCHEMA " );
            query.append( "\"" ).append( schema.getName() ).append( "\"" );
            if ( schema.getAuthorization() != null && !schema.getAuthorization().equals( "" ) ) {
                query.append( " AUTHORIZATION " ).append( schema.getAuthorization() );
            }
            try {
                int rows = executeSqlUpdate( transaction, query.toString() );
                transaction.commit();
                return new Result( new Debug().setAffectedRows( rows ) );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while creating a schema", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                return new Result( e );
            }
        }
        // drop schema
        else if ( !schema.isCreate() && schema.isDrop() ) {
            StringBuilder query = new StringBuilder( "DROP SCHEMA " );
            query.append( "\"" ).append( schema.getName() ).append( "\"" );
            if ( schema.isCascade() ) {
                query.append( " CASCADE" );
            }
            try {
                int rows = executeSqlUpdate( transaction, query.toString() );
                transaction.commit();
                return new Result( new Debug().setAffectedRows( rows ) );
            } catch ( TransactionException | QueryExecutionException e ) {
                log.error( "Caught exception while dropping a schema", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Could not rollback", ex );
                }
                return new Result( e );
            }
        } else {
            return new Result( "Neither the field 'create' nor the field 'drop' was set." );
        }
    }


    /**
     * Get all supported data types of the DBMS.
     */
    public Result getTypeInfo( final Request req, final Response res ) {
        ArrayList<String[]> data = new ArrayList<>();

        /*
        for ( PolyType polyType : PolyType.values() ) {
            // ignore types that are not relevant
            if ( polyType.getJdbcOrdinal() < -500 || polyType.getJdbcOrdinal() > 500 ) {
                continue;
            }
            String[] row = new String[1];
            for ( int i = 1; i <= 18; i++ ) {
                row[0] = polyType.name();
            }
            data.add( row );
        }
         */

        for ( PolyType polyType : PolyType.availableTypes() ) {
            String[] row = new String[1];
            row[0] = polyType.name();
            data.add( row );
        }

        DbColumn[] header = { new DbColumn( "TYPE_NAME" ) };
        return new Result( header, data.toArray( new String[0][1] ) );
    }


    /**
     * Get available actions for foreign key constraints
     */
    String[] getForeignKeyActions( Request req, Response res ) {
        ForeignKeyOption[] options = Catalog.ForeignKeyOption.values();
        String[] arr = new String[options.length];
        for ( int i = 0; i < options.length; i++ ) {
            arr[i] = options[i].name();
        }
        return arr;
    }


    /**
     * Send updates to the UI if Information objects in the query analyzer change.
     */
    @Override
    public void observeInfos( final Information info ) {
        try {
            WebSocket.broadcast( info.asJson() );
        } catch ( IOException e ) {
            log.error( "Caught exception during WebSocket broadcast", e );
        }
    }


    /**
     * Send an updated pageList of the query analyzer to the UI.
     */
    @Override
    public void observePageList( final String analyzerId, final InformationPage[] pages ) {
        ArrayList<SidebarElement> nodes = new ArrayList<>();
        int counter = 0;
        for ( InformationPage page : pages ) {
            nodes.add( new SidebarElement( page.getId(), page.getName(), analyzerId + "/", page.getIcon() ) );
            counter++;
        }
        try {
            WebSocket.sendPageList( this.gson.toJson( nodes.toArray( new SidebarElement[0] ) ) );
        } catch ( IOException e ) {
            log.error( "Caught exception during WebSocket broadcast", e );
        }
    }


    /**
     * Get the content of an InformationPage of a query analyzer.
     */
    public String getAnalyzerPage( final Request req, final Response res ) {
        String[] params = this.gson.fromJson( req.body(), String[].class );
        return InformationManager.getInstance( params[0] ).getPage( params[1] ).asJson();
    }


    /**
     * Close a query analyzer if not needed anymore.
     */
    public String closeAnalyzer( final Request req, final Response res ) {
        String id = req.body();
        InformationManager.close( id );
        return "";
    }


    /**
     * Import a dataset from Polypheny-Hub into Polypheny-DB
     */
    HubResult importDataset( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );
        String error = null;

        String randomFileName = UUID.randomUUID().toString();
        final String sysTempDir = System.getProperty( "java.io.tmpdir" );
        final File tempDir = new File( sysTempDir + File.separator + "hub" + File.separator + randomFileName + File.separator );
        if ( !tempDir.mkdirs() ) { // create folder
            log.error( "Unable to create temp folder: {}", tempDir.getAbsolutePath() );
            return new HubResult( "Unable to create temp folder" );
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
            String jsonFileName = "";
            String csvFileName = "";
            final File extractedFolder = new File( tempDir, "import" );
            if ( !extractedFolder.mkdirs() ) {
                log.error( "Unable to create folder for extracting files: {}", tempDir.getAbsolutePath() );
                return new HubResult( "Unable to create folder for extracting files" );
            }
            try ( ZipInputStream zis = new ZipInputStream( new FileInputStream( zipFile ) ) ) {
                ZipEntry zipEntry = zis.getNextEntry();
                while ( zipEntry != null ) {
                    if ( zipEntry.getName().endsWith( ".csv" ) ) {
                        csvFileName = zipEntry.getName();
                    } else if ( zipEntry.getName().endsWith( ".json" ) ) {
                        jsonFileName = zipEntry.getName();
                    }
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
            // table name
            String tableName = null;
            if ( request.tableName != null && request.tableName.trim().length() > 0 ) {
                tableName = request.tableName.trim();
            }
            // create table from .json file
            String json = new String( Files.readAllBytes( Paths.get( new File( extractedFolder, jsonFileName ).getPath() ) ), StandardCharsets.UTF_8 );
            JsonTable table = gson.fromJson( json, JsonTable.class );
            transaction = getTransaction();
            List<CatalogTable> tablesInSchema = catalog.getTables( new Catalog.Pattern( this.databaseName ), new Catalog.Pattern( request.schema ), null );
            int tableAlreadyExists = (int) tablesInSchema.stream().filter( t -> t.name.equals( table.tableName ) ).count();
            if ( tableAlreadyExists > 0 ) {
                return new HubResult( String.format( "Cannot import the dataset since the schema '%s' already contains a table with the name '%s'", request.schema, table.tableName ) );
            }

            String createTable = SchemaToJsonMapper.getCreateTableStatementFromJson( json, request.createPks, request.defaultValues, request.schema, tableName, request.store );
            executeSqlUpdate( transaction, createTable );

            // import data from .csv file
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
                    CSVReader csvReader = new CSVReader( reader );
            ) {
                long lineCount = Files.lines( new File( extractedFolder, csvFileName ).toPath() ).count();
                Status status = new Status( "tableImport", lineCount );
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
                        String insertQuery = String.format( "INSERT INTO \"%s\".\"%s\" %s %s", request.schema, table.tableName, columns, valueJoiner.toString() );
                        executeSqlUpdate( transaction, insertQuery );
                        valueJoiner = new StringJoiner( ",", "VALUES", "" );
                        status.setStatus( csvCounter );
                        WebSocket.broadcast( gson.toJson( status, Status.class ) );
                    }
                }
                if ( csvCounter % BATCH_SIZE != 0 ) {
                    String insertQuery = String.format( "INSERT INTO \"%s\".\"%s\" %s %s", request.schema, table.tableName, columns, valueJoiner.toString() );
                    executeSqlUpdate( transaction, insertQuery );
                    status.complete();
                    WebSocket.broadcast( gson.toJson( status, Status.class ) );
                }
            }

            transaction.commit();

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
        } catch ( GenericCatalogException e ) {
            log.error( "Could not export csv file", e );
            error = "Could not export csv file" + e.getMessage();
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rolling back transaction", e );
                }
            }
        } finally {
            // delete temp folder
            if ( !deleteDirectory( tempDir ) ) {
                log.error( "Unable to delete temp folder: {}", tempDir.getAbsolutePath() );
            }
        }

        if ( error != null ) {
            return new HubResult( error );
        } else {
            return new HubResult().setMessage( String.format( "Imported dataset into table %s on store %s", request.schema, request.store ) );
        }
    }


    /**
     * Export a table into a .zip consisting of a json file containing information of the table and columns and a csv files with the data
     */
    Result exportTable( final Request req, final Response res ) {
        HubRequest request = gson.fromJson( req.body(), HubRequest.class );
        Transaction transaction = getTransaction( false );

        String randomFileName = UUID.randomUUID().toString();
        final Charset charset = StandardCharsets.UTF_8;
        final String sysTempDir = System.getProperty( "java.io.tmpdir" );
        final File tempDir = new File( sysTempDir + File.separator + "hub" + File.separator + randomFileName + File.separator );
        if ( !tempDir.mkdirs() ) { // create folder
            log.error( "Unable to create temp folder: {}", tempDir.getAbsolutePath() );
            return new Result( "Unable to create temp folder" );
        }
        File tableFile = new File( tempDir, "table.csv" );
        File catalogFile = new File( tempDir, "catalog.json" );
        File zipFile = new File( tempDir, "table.zip" );
        try (
                OutputStreamWriter catalogWriter = new OutputStreamWriter( new FileOutputStream( catalogFile ), charset );
                FileOutputStream tableStream = new FileOutputStream( tableFile );
                FileOutputStream zipStream = new FileOutputStream( zipFile );
        ) {
            log.info( String.format( "Exporting %s.%s", request.schema, request.table ) );
            CatalogTable catalogTable = catalog.getTable( this.databaseName, request.schema, request.table );

            catalogWriter.write( SchemaToJsonMapper.exportTableDefinitionAsJson( catalogTable, request.createPks, request.defaultValues ) );
            catalogWriter.flush();

            String query = String.format( "SELECT * FROM \"%s\".\"%s\"", request.schema, request.table );
            // TODO use iterator instead of Result
            Result tableData = executeSqlSelect( transaction, new UIRequest(), query, true );
            transaction.commit();

            int totalRows = tableData.getData().length;
            int counter = 0;
            Status status = new Status( "tableExport", totalRows );
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
                if ( counter % 100 == 0 ) {
                    status.setStatus( counter );
                    WebSocket.broadcast( gson.toJson( status, Status.class ) );
                }
            }
            status.complete();
            WebSocket.broadcast( gson.toJson( status, Status.class ) );
            tableStream.flush();

            //from https://www.baeldung.com/java-compress-and-uncompress
            List<File> srcFiles = Arrays.asList( catalogFile, tableFile );
            try ( ZipOutputStream zipOut = new ZipOutputStream( zipStream, charset ) ) {
                for ( File fileToZip : srcFiles ) {
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

            //send file to php backend using Unirest
            HttpResponse jsonResponse = Unirest.post( request.hubLink )
                    .field( "action", "uploadDataset" )
                    .field( "userId", String.valueOf( request.userId ) )
                    .field( "secret", request.secret )
                    .field( "name", request.name )
                    .field( "pub", String.valueOf( request.pub ) )
                    .field( "dataset", zipFile )
                    .asString();

            // Get result
            StringWriter writer = new StringWriter();
            IOUtils.copy( jsonResponse.getRawBody(), writer );
            String resultString = writer.toString();
            log.info( String.format( "Exported %s.%s", request.schema, request.table ) );

            try {
                return gson.fromJson( resultString, Result.class );
            } catch ( JsonSyntaxException e ) {
                return new Result( resultString );
            }
        } catch ( TransactionException e ) {
            log.error( "Error while fetching table", e );
            return new Result( "Error while fetching table" );
        } catch ( IOException e ) {
            log.error( "Failed to write temporary file", e );
            return new Result( "Failed to write temporary file" );
        } catch ( Exception e ) {
            log.error( "Error while exporting table", e );
            return new Result( "Error while exporting table" );
        } finally {
            // delete temp folder
            if ( !deleteDirectory( tempDir ) ) {
                log.error( "Unable to delete temp folder: {}", tempDir.getAbsolutePath() );
            }
        }
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    /**
     * Execute a select statement with default limit
     */
    private Result executeSqlSelect( final Transaction transaction, final UIRequest request, final String sqlSelect ) throws QueryExecutionException {
        return executeSqlSelect( transaction, request, sqlSelect, false );
    }


    private Result executeSqlSelect( final Transaction transaction, final UIRequest request, final String sqlSelect, final boolean noLimit ) throws QueryExecutionException {
        // Parser Config
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        SqlParserConfig parserConfig = configConfigBuilder.build();

        PolyphenyDbSignature signature;
        List<List<Object>> rows;
        Iterator<Object> iterator = null;
        try {
            signature = processQuery( transaction, sqlSelect, parserConfig );
            final Enumerable enumerable = signature.enumerable( transaction.getDataContext() );
            //noinspection unchecked
            iterator = enumerable.iterator();
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            if ( noLimit ) {
                rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
            } else {
                rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, getPageSize() ), new ArrayList<>() );
            }
            stopWatch.stop();
            signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );
        } catch ( Throwable t ) {
            if ( iterator != null ) {
                try {
                    ((AutoCloseable) iterator).close();
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
            throw new QueryExecutionException( t );
        }

        try {
            CatalogTable catalogTable = null;
            if ( request.tableId != null ) {
                String[] t = request.tableId.split( "\\." );
                try {
                    catalogTable = catalog.getTable( this.databaseName, t[0], t[1] );
                } catch ( UnknownTableException | GenericCatalogException e ) {
                    log.error( "Caught exception", e );
                }
            }

            ArrayList<DbColumn> header = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {
                String columnName = metaData.columnName;

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
                        metaData.columnName,
                        metaData.type.name,
                        metaData.nullable == ResultSetMetaData.columnNullable,
                        metaData.displaySize,
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
                    } catch ( UnknownColumnException | GenericCatalogException | UnknownTableException e ) {
                        log.error( "Caught exception", e );
                    }
                }
                header.add( dbCol );
            }

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
                                temp[counter] = TimestampString.fromMillisSinceEpoch( (long) o ).toString();
                                break;
                            case "DATE":
                                temp[counter] = DateString.fromDaysSinceEpoch( (int) o ).toString();
                                break;
                            case "TIME":
                                temp[counter] = TimeString.fromMillisOfDay( (int) o ).toString();
                                break;
                            default:
                                temp[counter] = o.toString();
                        }
                    }
                    counter++;
                }
                data.add( temp );
            }

            return new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ) ).setInfo( new Debug().setAffectedRows( data.size() ) );
        } finally {
            try {
                ((AutoCloseable) iterator).close();
            } catch ( Exception e ) {
                log.error( "Exception while closing result iterator", e );
            }
        }
    }


    private PolyphenyDbSignature processQuery( Transaction transaction, String sql, SqlParserConfig parserConfig ) {
        PolyphenyDbSignature signature;
        transaction.resetQueryProcessor();
        SqlProcessor sqlProcessor = transaction.getSqlProcessor( parserConfig );

        SqlNode parsed = sqlProcessor.parse( sql );

        if ( parsed.isA( SqlKind.DDL ) ) {
            signature = sqlProcessor.prepareDdl( parsed );
        } else {
            Pair<SqlNode, RelDataType> validated = sqlProcessor.validate( parsed );
            RelRoot logicalRoot = sqlProcessor.translate( validated.left );

            // Prepare
            signature = transaction.getQueryProcessor().prepareQuery( logicalRoot );
        }
        return signature;
    }


    private int executeSqlUpdate( final Transaction transaction, final String sqlUpdate ) throws QueryExecutionException {
        // Parser Config
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        SqlParserConfig parserConfig = configConfigBuilder.build();

        PolyphenyDbSignature signature;
        try {
            signature = processQuery( transaction, sqlUpdate, parserConfig );
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        if ( signature.statementType == StatementType.OTHER_DDL ) {
            return 1;
        } else if ( signature.statementType == StatementType.IS_DML ) {
            Object object = signature.enumerable( transaction.getDataContext() ).iterator().next();
            if ( object != null && object.getClass().isArray() ) {
                Object[] o = (Object[]) object;
                return ((Number) o[0]).intValue();
            } else {
                return ((Number) object).intValue();
            }
        } else {
            throw new QueryExecutionException( "Unknown statement type: " + signature.statementType );
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
        Result result = executeSqlSelect( transaction, request, query );
        // We expect the result to be in the first column of the first row
        if ( result.getData().length == 0 ) {
            return 0;
        } else {
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
            if ( !entry.getValue().equals( "" ) ) {
                joiner.add( "CAST (\"" + entry.getKey() + "\" AS VARCHAR) LIKE '" + entry.getValue() + "%'" );
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


    private Transaction getTransaction() {
        return getTransaction( false );
    }


    private Transaction getTransaction( boolean analyze ) {
        try {
            return transactionManager.startTransaction( userName, databaseName, analyze );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    /**
     * Get the data types of each column of a table
     *
     * @param schemaName name of the schema
     * @param tableName name of the table
     * @return HashMap containing the type of each column. The key is the name of the column and the value is the Sql Type (java.sql.Types).
     */
    private Map<String, PolyType> getColumnTypes( String schemaName, String tableName ) {
        Map<String, PolyType> dataTypes = new HashMap<>();
        try {
            CatalogTable table = catalog.getTable( this.databaseName, schemaName, tableName );
            List<CatalogColumn> catalogColumns = catalog.getColumns( table.id );
            for ( CatalogColumn catalogColumn : catalogColumns ) {
                dataTypes.put( catalogColumn.name, catalogColumn.type );
            }
        } catch ( UnknownTableException | GenericCatalogException e ) {
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


    static class QueryExecutionException extends Exception {

        QueryExecutionException( String message ) {
            super( message );
        }


        QueryExecutionException( String message, Exception e ) {
            super( message, e );
        }


        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }

}
