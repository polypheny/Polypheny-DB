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

/*
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;


/**
 * Tool to run sql scripts on the catalog database.
 */
@Slf4j
class ScriptRunner {

    private static final String DEFAULT_DELIMITER = ";";


    static boolean runScript( Reader reader, TransactionHandler transactionHandler, boolean logComments ) {
        return runScript( reader, transactionHandler, logComments, DEFAULT_DELIMITER );
    }


    static boolean runScript( Reader reader, TransactionHandler transactionHandler, boolean logComments, String delimiter ) {
        StringBuilder builder = new StringBuilder();
        try ( LineNumberReader lineReader = new LineNumberReader( reader ) ) {
            String line;
            while ( (line = lineReader.readLine()) != null ) {
                line = line.trim();
                if ( line.startsWith( "--" ) ) {
                    // Comment
                    if ( logComments ) {
                        log.trace( "Ignoring SQL Comment in ScriptRunner: {}", line );
                    }
                } else if ( !line.isEmpty() && !line.startsWith( "//" ) ) {
                    if ( line.endsWith( delimiter ) ) {
                        builder.append( line.substring( 0, line.lastIndexOf( delimiter ) ) );
                        log.trace( "Executing SQL command in ScriptRunner: {}", builder.toString() );
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
            log.error( "Error while executing SQL script in ScriptRunner! Executing Rollback", e );
            try {
                transactionHandler.rollback();
            } catch ( CatalogTransactionException e1 ) {
                log.error( "Error while executing rollback in ScriptRunner!", e1 );
            }
            return false;
        }
        // ignore

    }


}
