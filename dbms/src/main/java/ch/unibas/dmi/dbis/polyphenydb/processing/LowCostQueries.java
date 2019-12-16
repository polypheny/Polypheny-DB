package ch.unibas.dmi.dbis.polyphenydb.processing;


import ch.unibas.dmi.dbis.polyphenydb.Authenticator;
import ch.unibas.dmi.dbis.polyphenydb.SqlProcessor;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiGroup;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiPage;
import ch.unibas.dmi.dbis.polyphenydb.information.Information;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationObserver;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.SqlParserConfig;
import ch.unibas.dmi.dbis.polyphenydb.util.LimitIterator;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.StatColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.StatResult;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.linq4j.Enumerable;


@Slf4j
public class LowCostQueries implements InformationObserver {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    /**
     * LowCostQueries can be used to retrieve short answered queries
     * Idea is to expose a selected list of sql operations with a small list of results to don't impact performance
     */
    public LowCostQueries( final TransactionManager transactionManager, String userName, String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;

        ConfigManager cm = ConfigManager.getInstance();
        // TODO: recycle config for webpages to limit values for the statistic queries
        cm.registerWebUiPage( new WebUiPage( "statistics", "Polypheny-DB Statistics", "Settings for the user interface." ) );
        cm.registerWebUiGroup( new WebUiGroup( "statisticView", "statistics" ).withTitle( "Statistics View" ) );
        cm.registerConfig( new ConfigInteger( "statisticSize", "Number of rows per page in the data view", 10 ).withUi( "statisticView" ) );
    }


    public LowCostQueries( TransactionManager transactionManager, Authenticator authenticator ) {
        this(transactionManager, "pa", "APP");
    }


    @Override
    public void observeInfos( Information info ) {

    }


    @Override
    public void observePageList( String debugId, InformationPage[] pages ) {

    }


    /**
     * Handles the request for one columns stats
     *
     * @param query the select query
     * @return result of the query
     */
    public StatColumn selectOneStat( String query ) {

        return this.executeSqlSelect( query ).getColumns()[0];
    }


    /**
     * Handles the request which retrieves the stats for multiple columns
     */
    private StatResult selectMultipleStats( String query ) {
        return this.executeSqlSelect( query );
    }


    private StatResult executeSqlSelect( String query ) {
        Transaction transaction = getTransaction();
        StatResult result = new StatResult( new StatColumn[]{} );

        try {
            result = executeSqlSelect( transaction, query );
            transaction.commit();
            transaction = getTransaction();

        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );

            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        }
        return result;
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private StatResult executeSqlSelect( final Transaction transaction, final String sqlSelect ) throws QueryExecutionException {
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
            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, getPageSize() ), new ArrayList<>() );
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

            ArrayList<DbColumn> header = new ArrayList<>();
            ArrayList<String> type = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {
                String columnName = metaData.columnName;

                type.add( metaData.type.name );

                DbColumn dbCol = new DbColumn(
                        metaData.columnName,
                        metaData.type.name,
                        metaData.nullable == ResultSetMetaData.columnNullable,
                        metaData.displaySize,
                        new SortState(),
                        "" );
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
            data.forEach( e -> {
                System.out.println( Arrays.toString( e ) );
            } );
            type.forEach( System.out::println );

            // TODO: own result object?
            return new StatResult( type, data.toArray( new String[0][] ) );
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


    /**
     * Get the number of rows that should be displayed in one page in the data view
     */
    private int getPageSize() {
        return ConfigManager.getInstance().getConfig( "pageSize" ).getInt();
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
