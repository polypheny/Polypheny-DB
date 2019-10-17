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

package ch.unibas.dmi.dbis.polyphenydb.sql.utils;


import static org.junit.Assert.assertNotNull;

import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserUtil.StringAndPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.utils.SqlTests.Stage;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.test.SqlTestFactory;


/**
 * Tester of {@link SqlValidator} and runtime execution of the input SQL.
 */
public class SqlRuntimeTester extends AbstractSqlTester {

    public SqlRuntimeTester( SqlTestFactory factory ) {
        super( factory );
    }


    @Override
    protected SqlTester with( SqlTestFactory factory ) {
        return new SqlRuntimeTester( factory );
    }


    @Override
    public void checkFails( String expression, String expectedError, boolean runtime ) {
        final String sql = runtime
                ? buildQuery2( expression )
                : buildQuery( expression );
        assertExceptionIsThrown( sql, expectedError, runtime );
    }


    @Override
    public void assertExceptionIsThrown( String sql, String expectedMsgPattern ) {
        assertExceptionIsThrown( sql, expectedMsgPattern, false );
    }


    public void assertExceptionIsThrown( String sql, String expectedMsgPattern, boolean runtime ) {
        final SqlNode sqlNode;
        final StringAndPos sap = SqlParserUtil.findPos( sql );
        try {
            sqlNode = parseQuery( sap.sql );
        } catch ( Throwable e ) {
            checkParseEx( e, expectedMsgPattern, sap.sql );
            return;
        }

        Throwable thrown = null;
        final Stage stage;
        final SqlValidator validator = getValidator();
        if ( runtime ) {
            stage = SqlTests.Stage.RUNTIME;
            SqlNode validated = validator.validate( sqlNode );
            assertNotNull( validated );
            try {
                check( sap.sql, SqlTests.ANY_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER, SqlTests.ANY_RESULT_CHECKER );
            } catch ( Throwable ex ) {
                // get the real exception in runtime check
                thrown = ex;
            }
        } else {
            stage = SqlTests.Stage.VALIDATE;
            try {
                validator.validate( sqlNode );
            } catch ( Throwable ex ) {
                thrown = ex;
            }
        }

        SqlTests.checkEx( thrown, expectedMsgPattern, sap, stage );
    }
}
