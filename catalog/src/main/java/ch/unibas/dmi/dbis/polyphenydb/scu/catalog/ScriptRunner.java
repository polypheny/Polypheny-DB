/*
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.scu.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to run sql scripts on the catalog database.
 */
class ScriptRunner {

    private static final Logger logger = LoggerFactory.getLogger( ScriptRunner.class );

    private static final String DEFAULT_DELIMITER = ";";


    static boolean runScript( Reader reader, TransactionHandler transactionHandler, boolean logComments ) {
        return runScript( reader, transactionHandler, logComments, DEFAULT_DELIMITER );
    }


    static boolean runScript( Reader reader, TransactionHandler transactionHandler, boolean logComments, String delimiter ) {
        StringBuilder builder = new StringBuilder();
        LineNumberReader lineReader = null;
        try {
            lineReader = new LineNumberReader( reader );
            String line;
            while ( (line = lineReader.readLine()) != null ) {
                line = line.trim();
                if ( line.startsWith( "--" ) ) {
                    // Comment
                    if ( logComments ) {
                        logger.trace( "Ignoring SQL Comment in ScriptRunner: {}", line );
                    }
                } else if ( !line.isEmpty() && !line.startsWith( "//" ) ) {
                    if ( line.endsWith( delimiter ) ) {
                        builder.append( line.substring( 0, line.lastIndexOf( delimiter ) ) );
                        logger.trace( "Executing SQL command in ScriptRunner: {}", builder.toString() );
                        transactionHandler.execute( builder.toString() );
                        builder = new StringBuilder();
                    } else {
                        builder.append( line ).append( " " );
                    }
                }
                Thread.yield();
            }
            return true;
        } catch ( SQLException | IOException e ) {
            logger.error( "Error while executing SQL script in ScriptRunner! Executing Rollback", e );
            try {
                transactionHandler.rollback();
            } catch ( CatalogTransactionException e1 ) {
                logger.error( "Error while executing rollback in ScriptRunner!", e1 );
            }
            return false;
        } finally {
            if ( lineReader != null ) {
                try {
                    lineReader.close();
                } catch ( IOException e ) {
                    // ignore
                }
            }
        }


    }


}
