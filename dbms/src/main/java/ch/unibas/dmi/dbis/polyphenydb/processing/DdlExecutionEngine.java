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

package ch.unibas.dmi.dbis.polyphenydb.processing;


import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbStatementHandle;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbContextException;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import java.util.LinkedList;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DdlExecutionEngine {

    private static final Logger LOG = LoggerFactory.getLogger( DdlExecutionEngine.class );

    private static DdlExecutionEngine INSTANCE;


    static {
        INSTANCE = new DdlExecutionEngine();
    }


    public static DdlExecutionEngine getInstance() {
        return INSTANCE;
    }


    private DdlExecutionEngine() {

    }


    public ExecuteResult execute( final StatementHandle h, final PolyphenyDbStatementHandle statement, final SqlNode parsed, final ContextImpl prepareContext ) {
        if ( parsed instanceof SqlExecutableStatement ) {
            try {
                ((SqlExecutableStatement) parsed).execute( prepareContext, CatalogManagerImpl.getInstance() );

                // Marshalling
                LinkedList<MetaResultSet> resultSets = new LinkedList<>();
                MetaResultSet resultSet = MetaResultSet.count( statement.getConnection().getConnectionId().toString(), h.id, 1 );
                resultSets.add( resultSet );
                //statement.setOpenResultSet( resultSet );

                return new ExecuteResult( resultSets );

            } catch ( PolyphenyDbContextException e ) { // If there is no exception, everything is fine and the dml query has successfully been executed
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            }
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits SqlExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


}
