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

/*
 *
 */

package org.polypheny.db.catalog;


import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.CatalogTransactionException;


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
