package ch.unibas.dmi.dbis.polyphenydb.scu.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of the TransactionHandler for local transaction (e.g., reads which must not be executed on all other nodes).
 */
class LocalTransactionHandler extends TransactionHandler {

    private static final Logger logger = LoggerFactory.getLogger( LocalTransactionHandler.class );

    private static final Queue<LocalTransactionHandler> freeInstances = new ConcurrentLinkedQueue<>();


    private LocalTransactionHandler() throws CatalogConnectionException {
        super();
        try {
            connection = Database.getInstance().getConnection();
            connection.setAutoCommit( false );
            statement = connection.createStatement();
        } catch ( SQLException e ) {
            throw new CatalogConnectionException( "Error while connecting to catalog storage", e );
        }
    }


    @Override
    boolean prepare() throws CatalogTransactionException {
        return false;
    }


    @Override
    void commit() throws CatalogTransactionException {
        try {
            connection.commit();
        } catch ( SQLException e ) {
            throw new CatalogTransactionException( "Error while committing transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    @Override
    void rollback() throws CatalogTransactionException {
        try {
            connection.rollback();
        } catch ( SQLException e ) {
            throw new CatalogTransactionException( "Error while rollback transaction in catalog storage", e );
        } finally {
            close();
        }
    }


    @SuppressWarnings("Duplicates")
    private void close() {
        try {
            if ( openStatements != null ) {
                for ( Statement openStatement : openStatements ) {
                    openStatement.close();
                }
            }
        } catch ( SQLException e ) {
            logger.debug( "Exception while closing connections in connection handler", e );
        } finally {
            openStatements = null;
            freeInstances.add( this );
        }
    }


    static LocalTransactionHandler getTransactionHandler() throws CatalogConnectionException {
        LocalTransactionHandler handler = freeInstances.poll();
        if ( handler == null ) {
            handler = new LocalTransactionHandler();
        }
        return handler;
    }


}
