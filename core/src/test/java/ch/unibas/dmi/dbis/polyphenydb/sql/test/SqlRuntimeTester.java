/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.test;


import static org.junit.Assert.assertNotNull;

import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserUtil.StringAndPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;


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
        final SqlTests.Stage stage;
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
