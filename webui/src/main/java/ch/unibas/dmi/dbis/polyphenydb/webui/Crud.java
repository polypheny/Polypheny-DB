/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.ConstraintType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.ForeignKeyOption;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogConstraint;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogIndex;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogPrimaryKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownIndexTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownKeyException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiGroup;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiPage;
import ch.unibas.dmi.dbis.polyphenydb.information.Information;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationHtml;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationObserver;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.util.LimitIterator;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbTable;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Debug;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Index;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Result;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ResultType;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Schema;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SidebarElement;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.TableConstraint;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.UIRelNode;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Uml;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.ColumnRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.ConstraintRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.EditTableRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.QueryRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.SchemaTreeRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.UIRequest;
import com.google.gson.Gson;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.commons.lang.math.NumberUtils;
import spark.Request;
import spark.Response;


@Slf4j
public class Crud implements InformationObserver {

    private final Gson gson = new Gson();
    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    /**
     * Constructor
     *
     * @param transactionManager The Polypheny-DB transaction manager
     */
    Crud( final TransactionManager transactionManager, final String userName, final String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;

        ConfigManager cm = ConfigManager.getInstance();
        cm.registerWebUiPage( new WebUiPage( "webUi", "Polypheny-DB UI", "Settings for the user interface." ) );
        cm.registerWebUiGroup( new WebUiGroup( "dataView", "webUi" ).withTitle( "Data View" ) );
        cm.registerConfig( new ConfigInteger( "pageSize", "Number of rows per page in the data view", 10 ).withUi( "dataView" ) );
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
        query.append( "SELECT * FROM " )
                .append( request.tableId )
                .append( where )
                .append( orderBy )
                .append( " LIMIT " )
                .append( getPageSize() )
                .append( " OFFSET " )
                .append( (Math.max( 0, request.currentPage - 1 )) * getPageSize() );

        try {
            result = executeSqlSelect( transaction, request, query.toString() );
            transaction.commit();
        } catch ( TransactionException | QueryExecutionException e ) {
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
            String[] t = request.tableId.split( "\\." );
            CatalogTable catalogTable = transaction.getCatalog().getTable( this.databaseName, t[0], t[1] );
            if ( catalogTable.tableType.equalsIgnoreCase( "TABLE" ) ) {
                result.setType( ResultType.TABLE );
            } else if ( catalogTable.tableType.equalsIgnoreCase( "VIEW" ) ) {
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
        return result;
    }


    ArrayList<SidebarElement> getSchemaTree( final Request req, final Response res ) {
        SchemaTreeRequest request = this.gson.fromJson( req.body(), SchemaTreeRequest.class );
        ArrayList<SidebarElement> result = new ArrayList<>();

        if ( request.depth < 1 ) {
            log.error( "Trying to fetch a schemaTree with depth < 1" );
            return new ArrayList<>();
        }

        Transaction transaction = getTransaction();
        try {

            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
            for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
                SidebarElement schemaTree = new SidebarElement( combinedSchema.getSchema().name, combinedSchema.getSchema().name, "", "cui-layers" );

                if ( request.depth > 1 ) {
                    ArrayList<SidebarElement> tables = new ArrayList<>();
                    ArrayList<SidebarElement> views = new ArrayList<>();
                    for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                        SidebarElement table = new SidebarElement( combinedSchema.getSchema().name + "." + combinedTable.getTable().name, combinedTable.getTable().name, request.routerLinkRoot, "fa fa-table" );

                        if ( request.depth > 2 ) {
                            for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
                                table.addChild( new SidebarElement( combinedSchema.getSchema().name + "." + combinedTable.getTable().name + "." + catalogColumn.name, catalogColumn.name, request.routerLinkRoot ).setCssClass( "sidebarColumn" ) );
                            }
                        }
                        if ( combinedTable.getTable().tableType.equals( "TABLE" ) ) {
                            tables.add( table );
                        } else if ( request.views && combinedTable.getTable().tableType.equals( "VIEW" ) ) {
                            views.add( table );
                        }
                    }
                    schemaTree.addChild( new SidebarElement( combinedSchema.getSchema().name + ".tables", "tables", request.routerLinkRoot, "fa fa-table" ).addChildren( tables ).setRouterLink( "" ) );
                    if ( request.views ) {
                        schemaTree.addChild( new SidebarElement( combinedSchema.getSchema().name + ".views", "views", request.routerLinkRoot, "icon-eye" ).addChildren( views ).setRouterLink( "" ) );
                    }
                }
                result.add( schemaTree );
            }
            transaction.commit();
        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | GenericCatalogException | TransactionException e ) {
            log.error( "Caught exception", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }

        return result;
    }


    /**
     * Get all tables of a schema
     */
    Result getTables( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        try {
            List<CatalogTable> tables = transaction.getCatalog().getTables( new Catalog.Pattern( databaseName ), new Catalog.Pattern( request.schema ), null );
            ArrayList<String> tableNames = new ArrayList<>();
            for ( CatalogTable catalogTable : tables ) {
                tableNames.add( catalogTable.name );
            }
            result = new Result( new Debug().setAffectedRows( tableNames.size() ) ).setTables( tableNames );
            transaction.commit();
        } catch ( GenericCatalogException | TransactionException e ) {
            log.error( "Caught exception while fetching tables", e );
            result = new Result( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
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
        query.append( request.schema ).append( "." ).append( request.table );
        try {
            int a = executeSqlUpdate( transaction, query.toString() );
            result = new Result( new Debug().setAffectedRows( a ).setGeneratedQuery( query.toString() ) );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping or truncating a table", e );
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
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
        query.append( "CREATE TABLE " ).append( request.schema ).append( "." ).append( request.table ).append( "(" );
        StringBuilder colBuilder;
        Result result;
        StringJoiner primaryKeys = new StringJoiner( ",", "PRIMARY KEY (", ")" );
        int primaryCounter = 0;
        for ( DbColumn col : request.columns ) {
            colBuilder = new StringBuilder();
            colBuilder.append( col.name ).append( " " ).append( col.dataType );
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
                        // varchar, timestamptz, bool
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

        try {
            int a = executeSqlUpdate( transaction, query.toString() );
            result = new Result( new Debug().setGeneratedQuery( query.toString() ).setAffectedRows( a ) );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating a table", e );
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback CREATE TABLE statement: " + ex.getMessage(), ex );
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
        query.append( "INSERT INTO " ).append( request.tableId );
        StringJoiner values = new StringJoiner( ",", "(", ")" );

        String[] t = request.tableId.split( "\\." );
        Map<String, PolySqlType> dataTypes = getColumnTypes( t[0], t[1] );
        for ( Map.Entry<String, String> entry : request.data.entrySet() ) {
            cols.add( entry.getKey() );
            String value = entry.getValue();
            if ( value == null ) {
                value = "NULL";
            } else if ( dataTypes.get( entry.getKey() ).isCharType() ) {
                value = "'" + value + "'";
            }
            values.add( value );
        }
        query.append( cols.toString() );
        query.append( " VALUES " ).append( values.toString() );

        try {
            rowsAffected = executeSqlUpdate( transaction, query.toString() );
            result = new Result( new Debug().setAffectedRows( rowsAffected ).setGeneratedQuery( query.toString() ) );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while inserting a row", e );
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
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

        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed and in case of auto commit of, the information is overwritten
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
        allQueries = allQueries.replaceAll( "(?s)(\\/\\*.*?\\*\\/)", "" );
        allQueries = allQueries.replaceAll( "(?m)(--.*?$)", "" );
        String[] queries = allQueries.split( ";", 0 );
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
                // decrease limit if it is too large
                else {
                    Pattern pattern = Pattern.compile( "(.*?LIMIT[\\s+])(\\d+)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
                    Matcher limitMatcher = pattern.matcher( query );
                    if ( limitMatcher.find() ) {
                        int limit = Integer.parseInt( limitMatcher.group( 2 ) );
                        if ( limit > getPageSize() ) {
                            // see https://stackoverflow.com/questions/38296673/replace-group-1-of-java-regex-with-out-replacing-the-entire-regex?rq=1
                            query = limitMatcher.replaceFirst( "$1 " + getPageSize() );
                        }
                    }
                }
                try {
                    temp = System.nanoTime();
                    result = executeSqlSelect( transaction, request, query ).setInfo( new Debug().setGeneratedQuery( query ) );
                    executionTime += System.nanoTime() - temp;
                    results.add( result );
                    if ( autoCommit ) {
                        transaction.commit();
                        transaction = getTransaction( request.analyze );
                    }
                } catch ( QueryExecutionException | TransactionException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query ) );
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
                } catch ( QueryExecutionException | TransactionException e ) {
                    log.error( "Caught exception while executing a query from the console", e );
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query ) );
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
            results.add( new Result( e.getMessage() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }

        if ( queryAnalyzer != null ) {
            InformationPage p1 = new InformationPage( "p1", "Query analysis", "Analysis of the query." );
            InformationGroup g1 = new InformationGroup( "Execution time", "p1" );
            InformationHtml html;
            if ( executionTime < 1e4 ) {
                html = new InformationHtml( "exec_time", "Execution time", String.format( "Execution time: %d nanoseconds", executionTime ) );
            } else {
                long millis = TimeUnit.MILLISECONDS.convert( executionTime, TimeUnit.NANOSECONDS );
                // format time: see: https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java#answer-625444
                DateFormat df = new SimpleDateFormat( "m 'min' s 'sec' S 'ms'" );
                String durationText = df.format( new Date( millis ) );
                html = new InformationHtml( "exec_time", "Execution time", String.format( "Execution time: %s", durationText ) );
            }
            queryAnalyzer.addPage( p1 );
            queryAnalyzer.addGroup( g1 );
            queryAnalyzer.registerInformation( html );
        }

        return results;
    }


    /**
     * Delete a row from a table.
     * The row is determined by the value of every column in that row (conjunction).
     * The transaction is being rolled back, if more that one row would be deleted.
     * TODO: This is not a nice solution
     */
    Result deleteRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Transaction transaction = getTransaction();
        Result result;
        StringBuilder builder = new StringBuilder();
        builder.append( "DELETE FROM " ).append( request.tableId ).append( " WHERE " );
        StringJoiner joiner = new StringJoiner( " AND ", "", "" );
        String[] t = request.tableId.split( "\\." );
        Map<String, PolySqlType> dataTypes = getColumnTypes( t[0], t[1] );
        for ( Entry<String, String> entry : request.data.entrySet() ) {
            String condition = "";
            if ( entry.getValue() == null ) {
                condition = String.format( "%s IS NULL", entry.getKey() );
            } else if ( !dataTypes.get( entry.getKey() ).isCharType() ) {
                condition = String.format( "%s = %s", entry.getKey(), entry.getValue() );
            } else {
                condition = String.format( "%s = '%s'", entry.getKey(), entry.getValue() );
            }
            joiner.add( condition );
        }
        builder.append( joiner.toString() );

        try {
            int numOfRows = executeSqlUpdate( transaction, builder.toString() );
            // only commit if one row is deleted
            if ( numOfRows == 1 ) {
                transaction.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                transaction.rollback();
                result = new Result( "Attempt to delete " + numOfRows + " rows was blocked." ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while deleting a row", e );
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
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
        builder.append( "UPDATE " ).append( request.tableId ).append( " SET " );
        StringJoiner setStatements = new StringJoiner( ",", "", "" );
        for ( Entry<String, String> entry : request.data.entrySet() ) {
            if ( entry.getValue() == null ) {
                setStatements.add( String.format( "%s = NULL", entry.getKey() ) );
            } else if ( NumberUtils.isNumber( entry.getValue() ) ) {
                setStatements.add( String.format( "%s = %s", entry.getKey(), entry.getValue() ) );
            } else {
                setStatements.add( String.format( "%s = '%s'", entry.getKey(), entry.getValue() ) );
            }
        }
        builder.append( setStatements.toString() );

        StringJoiner where = new StringJoiner( " AND ", "", "" );
        for ( Entry<String, String> entry : request.filter.entrySet() ) {
            where.add( String.format( "%s = '%s'", entry.getKey(), entry.getValue() ) );
        }
        builder.append( " WHERE " ).append( where.toString() );

        try {
            int numOfRows = executeSqlUpdate( transaction, builder.toString() );

            if ( numOfRows == 1 ) {
                transaction.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                transaction.rollback();
                result = new Result( "Attempt to update " + numOfRows + " rows was blocked." ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while updating a row", e );
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
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
        Transaction transaction = getTransaction();
        Result result;

        String[] t = request.tableId.split( "\\." );
        ArrayList<DbColumn> cols = new ArrayList<>();

        try {
            CatalogTable catalogTable = transaction.getCatalog().getTable( databaseName, t[0], t[1] );
            CatalogCombinedTable combinedTable = transaction.getCatalog().getCombinedTable( catalogTable.id );
            ArrayList<String> primaryColumns;
            if ( catalogTable.primaryKey != null ) {
                CatalogPrimaryKey primaryKey = transaction.getCatalog().getPrimaryKey( catalogTable.primaryKey );
                primaryColumns = new ArrayList<>( primaryKey.columnNames );
            } else {
                primaryColumns = new ArrayList<>();
            }
            for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
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
            transaction.commit();
        } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException | TransactionException e ) {
            log.error( "Caught exception while getting a column", e );
            result = new Result( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
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

        // rename column if needed
        if ( !oldColumn.name.equals( newColumn.name ) ) {
            String query = String.format( "ALTER TABLE %s RENAME COLUMN %s TO %s", request.tableId, oldColumn.name, newColumn.name );
            queries.add( query );
        }

        // change type + length
        // TODO: cast if needed
        if ( !oldColumn.dataType.equals( newColumn.dataType ) || !Objects.equals( oldColumn.maxLength, newColumn.maxLength ) ) {
            if ( newColumn.maxLength != null ) {
                String query = String.format( "ALTER TABLE %s MODIFY COLUMN %s SET TYPE %s(%s)", request.tableId, newColumn.name, newColumn.dataType, newColumn.maxLength );
                queries.add( query );
            } else {
                // TODO: drop maxlength if requested
                String query = String.format( "ALTER TABLE %s MODIFY COLUMN %s SET TYPE %s", request.tableId, newColumn.name, newColumn.dataType );
                queries.add( query );
            }
        }

        // set/drop nullable
        if ( oldColumn.nullable != newColumn.nullable ) {
            String nullable = "SET";
            if ( newColumn.nullable ) {
                nullable = "DROP";
            }
            String query = "ALTER TABLE " + request.tableId + " MODIFY COLUMN " + newColumn.name + " " + nullable + " NOT NULL";
            queries.add( query );
        }

        // change default value
        if ( oldColumn.defaultValue == null || newColumn.defaultValue == null || !oldColumn.defaultValue.equals( newColumn.defaultValue ) ) {
            String query;
            if ( newColumn.defaultValue == null ) {
                query = String.format( "ALTER TABLE %s MODIFY COLUMN %s DROP DEFAULT", request.tableId, newColumn.name );
            } else {
                query = String.format( "ALTER TABLE %s MODIFY COLUMN %s SET DEFAULT ", request.tableId, newColumn.name );
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
            result = new Result( e.toString() ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() ) );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught exception during rollback", e2 );
                result = new Result( e2.toString() ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() ) );
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

        String query = String.format( "ALTER TABLE %s ADD COLUMN %s %s", request.tableId, request.newColumn.name, request.newColumn.dataType );
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
            result = new Result( e.getMessage() );
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

        Result result;
        String query = String.format( "ALTER TABLE %s DROP COLUMN %s", request.tableId, request.oldColumn.name );
        try {
            int affectedRows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( affectedRows ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a column", e );
            result = new Result( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Get constraints of a table
     */
    Result getConstraints( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Transaction transaction = getTransaction();
        Result result;

        String[] t = request.tableId.split( "\\." );
        ArrayList<TableConstraint> resultList = new ArrayList<>();
        Map<String, ArrayList<String>> temp = new HashMap<>();

        try {
            CatalogTable catalogTable = transaction.getCatalog().getTable( databaseName, t[0], t[1] );

            // get primary key
            if ( catalogTable.primaryKey != null ) {
                CatalogPrimaryKey primaryKey = transaction.getCatalog().getPrimaryKey( catalogTable.primaryKey );
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
            List<CatalogConstraint> constraints = transaction.getCatalog().getConstraints( catalogTable.id );
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
            List<CatalogForeignKey> foreignKeys = transaction.getCatalog().getForeignKeys( catalogTable.id );
            for ( CatalogForeignKey catalogForeignKey : foreignKeys ) {
                temp.put( catalogForeignKey.name, new ArrayList<>( catalogForeignKey.columnNames ) );
            }
            for ( Map.Entry<String, ArrayList<String>> entry : temp.entrySet() ) {
                resultList.add( new TableConstraint( entry.getKey(), "FOREIGN KEY", entry.getValue() ) );
            }

            DbColumn[] header = { new DbColumn( "Constraint name" ), new DbColumn( "Constraint type" ), new DbColumn( "Columns" ) };
            ArrayList<String[]> data = new ArrayList<>();
            resultList.forEach( ( c ) -> {
                data.add( c.asRow() );
            } );

            result = new Result( header, data.toArray( new String[0][2] ) );
            transaction.commit();
        } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException | TransactionException e ) {
            log.error( "Caught exception while fetching constraints", e );
            result = new Result( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }

        return result;
    }


    Result dropConstraint( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
        Transaction transaction = getTransaction();

        String query;
        if ( request.constraint.type.equals( "PRIMARY KEY" ) ) {
            query = String.format( "ALTER TABLE %s DROP PRIMARY KEY", request.table );
        } else if ( request.constraint.type.equals( "FOREIGN KEY" ) ) {
            query = String.format( "ALTER TABLE %s DROP FOREIGN KEY %s", request.table, request.constraint.name );
        } else {
            query = String.format( "ALTER TABLE %s DROP CONSTRAINT %s", request.table, request.constraint.name );
        }
        Result result;
        try {
            int rows = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( rows ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping a constraint", e );
            result = new Result( e.getMessage() );
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

        Result result;
        if ( request.constraint.columns.length > 0 ) {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( String s : request.constraint.columns ) {
                joiner.add( s );
            }
            String query = "ALTER TABLE " + request.table + " ADD PRIMARY KEY " + joiner.toString();
            try {
                int rows = executeSqlUpdate( transaction, query );
                transaction.commit();
                result = new Result( new Debug().setAffectedRows( rows ).setGeneratedQuery( query ) );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while adding a primary key", e );
                result = new Result( e.getMessage() );
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

        Result result;
        if ( request.constraint.columns.length > 0 ) {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( String s : request.constraint.columns ) {
                joiner.add( s );
            }
            String query = "ALTER TABLE " + request.table + " ADD CONSTRAINT " + request.constraint.name + " UNIQUE " + joiner.toString();
            try {
                int rows = executeSqlUpdate( transaction, query );
                transaction.commit();
                result = new Result( new Debug().setAffectedRows( rows ).setGeneratedQuery( query ) );
            } catch ( QueryExecutionException | TransactionException e ) {
                log.error( "Caught exception while adding a unique constraint", e );
                result = new Result( e.getMessage() );
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
        Transaction transaction = getTransaction();
        Result result;
        try {
            CatalogTable catalogTable = transaction.getCatalog().getTable( databaseName, request.schema, request.table );
            List<CatalogIndex> catalogIndexes = transaction.getCatalog().getIndexes( catalogTable.id, false );

            DbColumn[] header = { new DbColumn( "name" ), new DbColumn( "columns" ), new DbColumn( "type" ) };

            ArrayList<String[]> data = new ArrayList<>();
            for ( CatalogIndex catalogIndex : catalogIndexes ) {
                String[] arr = new String[3];
                arr[0] = catalogIndex.name;
                arr[1] = String.join( ", ", catalogIndex.key.columnNames );
                arr[2] = Catalog.IndexType.getById( catalogIndex.type ).toString();
                data.add( arr );
            }

            result = new Result( header, data.toArray( new String[0][2] ) );
            transaction.commit();
        } catch ( UnknownTableException | GenericCatalogException | TransactionException | UnknownIndexTypeException e ) {
            log.error( "Caught exception while fetching indexes", e );
            result = new Result( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }


    /**
     * Drop an index of a table
     */
    Result dropIndex( final Request req, final Response res ) {
        Index index = gson.fromJson( req.body(), Index.class );
        Transaction transaction = getTransaction();

        String query = String.format( "ALTER TABLE %s DROP INDEX %s", index.getTable(), index.getName() );
        Result result;
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setGeneratedQuery( query ).setAffectedRows( a ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while dropping an index", e );
            result = new Result( e.getMessage() );
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

        Result result;
        StringJoiner colJoiner = new StringJoiner( ",", "(", ")" );
        for ( String col : index.getColumns() ) {
            colJoiner.add( col );
        }
        String query = String.format( "ALTER TABLE %s.%s ADD INDEX %s ON %s USING %s", index.getSchema(), index.getTable(), index.getName(), colJoiner.toString(), index.getMethod() );
        try {
            int a = executeSqlUpdate( transaction, query );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( a ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating an index", e );
            result = new Result( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }
        return result;
    }


    /**
     * Get the required information for the uml view:
     * Foreign keys
     * Tables with its columns
     */
    Uml getUml( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Transaction transaction = getTransaction();
        ArrayList<ForeignKey> fKeys = new ArrayList<>();
        ArrayList<DbTable> tables = new ArrayList<>();

        try {
            List<CatalogTable> catalogTables = transaction.getCatalog().getTables( new Catalog.Pattern( databaseName ), new Catalog.Pattern( request.schema ), null );
            for ( CatalogTable catalogTable : catalogTables ) {
                if ( catalogTable.tableType.equalsIgnoreCase( "TABLE" ) ) {
                    // get foreign keys
                    List<CatalogForeignKey> foreignKeys = transaction.getCatalog().getForeignKeys( catalogTable.id );
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
                    CatalogCombinedTable combinedTable = transaction.getCatalog().getCombinedTable( catalogTable.id );
                    DbTable table = new DbTable( catalogTable.name, catalogTable.schemaName );
                    for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
                        table.addColumn( new DbColumn( catalogColumn.name ) );
                    }

                    // get primary key with its columns
                    if ( catalogTable.primaryKey != null ) {
                        CatalogPrimaryKey catalogPrimaryKey = transaction.getCatalog().getPrimaryKey( catalogTable.primaryKey );
                        for ( String columnName : catalogPrimaryKey.columnNames ) {
                            table.addPrimaryKeyField( columnName );
                        }
                    }

                    // get unique constraints
                    List<CatalogConstraint> catalogConstraints = transaction.getCatalog().getConstraints( catalogTable.id );
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
                    List<CatalogIndex> catalogIndexes = transaction.getCatalog().getIndexes( catalogTable.id, true );
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
            transaction.commit();
        } catch ( GenericCatalogException | UnknownTableException | TransactionException | UnknownKeyException e ) {
            log.error( "Could not fetch foreign keys of the schema " + request.schema, e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }

        return new Uml( tables, fKeys );
    }


    /**
     * Add foreign key
     */
    Result addForeignKey( final Request req, final Response res ) {
        ForeignKey fk = this.gson.fromJson( req.body(), ForeignKey.class );
        Transaction transaction = getTransaction();

        Result result;
        try {
            String sql = String.format( "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s) ON UPDATE %s ON DELETE %s",
                    fk.getFkTableName(), fk.getFkName(), fk.getFkColumnName(), fk.getPkTableName(), fk.getPkColumnName(), fk.getUpdate(), fk.getDelete() );
            executeSqlUpdate( transaction, sql );
            transaction.commit();
            result = new Result( new Debug().setAffectedRows( 1 ) );
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while adding a foreign key", e );
            result = new Result( e.getMessage() );
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

        RelNode result;
        try {
            result = QueryPlanBuilder.buildFromTree( topNode, transaction );
        } catch ( Exception e ) {
            log.error( "Caught exception while building the plan builder tree", e );
            return new Result( e.getMessage() );
        }

        PolyphenyDbSignature signature = transaction.getQueryProcessor().processQuery( result );

        List<List<Object>> rows;
        try {
            @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( transaction.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, getPageSize() ), new ArrayList<>() );
        } catch ( Exception e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            return new Result( e.getMessage() );
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
            if ( schema.isIfExists() ) {
                query.append( "IF NOT EXISTS " );
            }
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
                return new Result( e.getMessage() );
            }
        }
        // drop schema
        else if ( !schema.isCreate() && schema.isDrop() ) {
            StringBuilder query = new StringBuilder( "DROP SCHEMA " );
            if ( schema.isIfExists() ) {
                query.append( "IF EXISTS " );
            }
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
                return new Result( e.getMessage() );
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
        for ( SqlTypeName sqlTypeName : SqlTypeName.values() ) {
            // ignore types that are not relevant
            if ( sqlTypeName.getJdbcOrdinal() < -500 || sqlTypeName.getJdbcOrdinal() > 500 ) {
                continue;
            }
            String[] row = new String[1];
            for ( int i = 1; i <= 18; i++ ) {
                row[0] = sqlTypeName.name();
            }
            data.add( row );
        }
         */

        for ( PolySqlType polySqlType : PolySqlType.values() ) {
            String[] row = new String[1];
            row[0] = polySqlType.name();
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

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private Result executeSqlSelect( final Transaction transaction, final UIRequest request, final String sqlSelect ) throws QueryExecutionException {
        // Parser Config
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        SqlParser.Config parserConfig = configConfigBuilder.build();

        PolyphenyDbSignature signature;
        try {
            signature = transaction.getQueryProcessor().processSqlQuery( sqlSelect, parserConfig );
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        List<List<Object>> rows;
        @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( transaction.getDataContext() );
        Iterator<Object> iterator = iterable.iterator();
        rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, getPageSize() ), new ArrayList<>() );

        CatalogTable catalogTable = null;
        if ( request.tableId != null ) {
            String[] t = request.tableId.split( "\\." );
            try {
                catalogTable = transaction.getCatalog().getTable( this.databaseName, t[0], t[1] );
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
                    if ( transaction.getCatalog().checkIfExistsColumn( catalogTable.id, columnName ) ) {
                        CatalogColumn catalogColumn = transaction.getCatalog().getColumn( catalogTable.id, columnName );
                        if ( catalogColumn.defaultValue != null ) {
                            dbCol.defaultValue = catalogColumn.defaultValue.value;
                        }
                    }
                } catch ( UnknownColumnException | GenericCatalogException e ) {
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
                    temp[counter] = o.toString();
                }
                counter++;
            }
            data.add( temp );
        }

        return new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ) ).setInfo( new Debug().setAffectedRows( data.size() ) );
    }


    private int executeSqlUpdate( final Transaction transaction, final String sqlUpdate ) throws QueryExecutionException {
        // Parser Config
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
        configConfigBuilder.setUnquotedCasing( Casing.TO_LOWER );
        configConfigBuilder.setQuotedCasing( Casing.TO_LOWER );
        SqlParser.Config parserConfig = configConfigBuilder.build();

        PolyphenyDbSignature signature;
        try {
            signature = transaction.getQueryProcessor().processSqlQuery( sqlUpdate, parserConfig );
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        if ( signature.statementType == StatementType.OTHER_DDL ) {
            return 1;
        } else if ( signature.statementType == StatementType.IS_DML ) {
            return ((Number) signature.enumerable( transaction.getDataContext() ).iterator().next()).intValue();
        } else {
            throw new QueryExecutionException( "Unknown statement type: " + signature.statementType );
        }
    }


    /**
     * Get the Number of rows in a table
     */
    private int getTableSize( Transaction transaction, final UIRequest request ) throws QueryExecutionException {
        String query = "SELECT count(*) FROM " + request.tableId;
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
        return ConfigManager.getInstance().getConfig( "pageSize" ).getInt();
    }


    private String filterTable( final Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", " WHERE ", "" );
        int counter = 0;
        for ( Map.Entry<String, String> entry : filter.entrySet() ) {
            if ( !entry.getValue().equals( "" ) ) {
                joiner.add( "CAST (" + entry.getKey() + " AS VARCHAR) LIKE '" + entry.getValue() + "%'" );
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
                joiner.add( entry.getKey() + " " + entry.getValue().direction );
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
    private Map<String, PolySqlType> getColumnTypes( String schemaName, String tableName ) {
        Map<String, PolySqlType> dataTypes = new HashMap<>();
        Transaction transaction = getTransaction();
        try {
            CatalogTable table = transaction.getCatalog().getTable( this.databaseName, schemaName, tableName );
            List<CatalogColumn> catalogColumns = transaction.getCatalog().getColumns( table.id );
            for ( CatalogColumn catalogColumn : catalogColumns ) {
                dataTypes.put( catalogColumn.name, catalogColumn.type );
            }
        } catch ( UnknownTableException | GenericCatalogException | UnknownCollationException | UnknownTypeException e ) {
            log.error( "Caught exception", e );
        }
        return dataTypes;
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
