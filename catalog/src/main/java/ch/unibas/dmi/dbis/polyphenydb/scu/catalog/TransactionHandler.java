package ch.unibas.dmi.dbis.polyphenydb.scu.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a transaction and provides methods to interact with the database system.
 */
abstract class TransactionHandler {

    private static final Logger logger = LoggerFactory.getLogger( TransactionHandler.class );

    Connection connection;
    Statement statement;

    /**
     * List of all statements which have to be closed to free resources
     */
    List<Statement> openStatements;


    int executeUpdate( final String sql ) throws SQLException {
        logger.trace( "Executing query on catalog database: " + sql );
        return statement.executeUpdate( sql );
    }


    ResultSet executeSelect( final String sql ) throws SQLException {
        logger.trace( "Executing query on catalog database: " + sql );
        return createStatement().executeQuery( sql );
    }


    void execute( final String sql ) throws SQLException {
        logger.trace( "Executing query on catalog database: " + sql );
        statement.execute( sql );
    }


    abstract boolean prepare() throws CatalogTransactionException;

    abstract void commit() throws CatalogTransactionException;

    abstract void rollback() throws CatalogTransactionException;


    private Statement createStatement() throws SQLException {
        if ( openStatements == null ) {
            openStatements = new LinkedList<>();
        }
        Statement statement = connection.createStatement();
        openStatements.add( statement );
        return statement;
    }

}
