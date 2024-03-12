/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.language.utils;


import java.lang.reflect.Method;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlTestFactory;
import org.polypheny.db.sql.language.parser.SqlParserUtil;
import org.polypheny.db.sql.language.utils.SqlValidatorTestCase.TesterConfigurationExtension;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.util.Conformance;


/**
 * An abstract base class for implementing tests against {@link SqlValidator}.
 * <p>
 * A derived class can refine this test in two ways. First, it can add <code>testXxx()</code> methods, to test more functionality.
 * <p>
 * Second, it can override the {@link #getTester} method to return a different implementation of the {@link Tester} object. This encapsulates the differences between test environments, for example, which SQL parser or validator to use.
 */
@ExtendWith(TesterConfigurationExtension.class)
public class SqlValidatorTestCase extends SqlLanguageDependent {

    protected SqlTester tester;


    /**
     * Creates a test case.
     */
    public SqlValidatorTestCase() {
        this.tester = getTester();
    }


    /**
     * Returns a tester. Derived classes should override this method to run the same set of tests in a different testing environment.
     */
    public SqlTester getTester() {
        return new SqlValidatorTester( SqlTestFactory.INSTANCE );
    }


    public final Sql sql( String sql ) {
        return new Sql( tester, sql, true );
    }


    public final Sql expr( String sql ) {
        return new Sql( tester, sql, false );
    }


    public void check( String sql ) {
        sql( sql ).ok();
    }


    /**
     * Checks that a SQL query gives a particular error, or succeeds if {@code expected} is null.
     */
    public final void checkFails( String sql, String expected ) {
        sql( sql ).fails( expected );
    }


    /**
     * Checks whether an exception matches the expected pattern. If <code>sap</code> contains an error location, checks this too.
     *
     * @param ex Exception thrown
     * @param expectedMsgPattern Expected pattern
     * @param sap Query and (optional) position in query
     */
    public static void checkEx( Throwable ex, String expectedMsgPattern, SqlParserUtil.StringAndPos sap ) {
        SqlTests.checkEx( ex, expectedMsgPattern, sap, SqlTests.Stage.VALIDATE );
    }


    /**
     * Encapsulates differences between test environments, for example, which SQL parser or validator to use.
     * <p>
     * It contains a mock schema with <code>EMP</code> and <code>DEPT</code> tables, which can run without having to start up Farrago.
     */
    public interface Tester {

        SqlNode parseQuery( String sql ) throws NodeParseException;

        SqlNode parseAndValidate( SqlValidator validator, String sql );

        SqlValidator getValidator();

        /**
         * Checks that a query is valid, or, if invalid, throws the right message at the right location.
         * <p>
         * If <code>expectedMsgPattern</code> is null, the query must succeed.
         * <p>
         * If <code>expectedMsgPattern</code> is not null, the query must fail, and give an error location of (expectedLine, expectedColumn) through (expectedEndLine, expectedEndColumn).
         *
         * @param sql SQL statement
         * @param expectedMsgPattern If this parameter is null the query must be valid for the test to pass; If this parameter is not null the query must be malformed and the message given must match the pattern
         */
        void assertExceptionIsThrown( String sql, String expectedMsgPattern );

        /**
         * Returns the data type of the sole column of a SQL query.
         * <p>
         * For example, <code>getResultType("VALUES (1")</code> returns <code>INTEGER</code>.
         * <p>
         * Fails if query returns more than one column.
         *
         * @see #getResultType(String)
         */
        AlgDataType getColumnType( String sql );

        /**
         * Returns the data type of the row returned by a SQL query.
         * <p>
         * For example, <code>getResultType("VALUES (1, 'foo')")</code> returns <code>RecordType(INTEGER EXPR$0, CHAR(3) EXPR#1)</code>.
         */
        AlgDataType getResultType( String sql );

        /**
         * Checks that a query returns one column of an expected type. For example, <code>checkType("VALUES (1 + 2)", "INTEGER NOT NULL")</code>.
         */
        void checkColumnType( String sql, String expected );

        /**
         * Checks that a query returns one column of an expected type. For example, <code>checkType("select empno, name from emp""{EMPNO INTEGER NOT NULL, NAME VARCHAR(10) NOT NULL}")</code>.
         */
        void checkResultType( String sql, String expected );

        Conformance getConformance();

    }


    /**
     * Fluent testing API.
     */
    public static class Sql {

        private final SqlTester tester;
        private final String sql;


        /**
         * Creates a Sql.
         *
         * @param tester Tester
         * @param sql SQL query or expression
         * @param query True if {@code sql} is a query, false if it is an expression
         */
        Sql( SqlTester tester, String sql, boolean query ) {
            this.tester = tester;
            this.sql = query ? sql : AbstractSqlTester.buildQuery( sql );
        }


        public Sql sql( String sql ) {
            return new Sql( tester, sql, true );
        }


        Sql ok() {
            tester.assertExceptionIsThrown( sql, null );
            return this;
        }


        Sql fails( String expected ) {
            tester.assertExceptionIsThrown( sql, expected );
            return this;
        }


        public Sql type( String expectedType ) {
            tester.checkResultType( sql, expectedType );
            return this;
        }


    }


    /**
     * Enables to configure {@link #tester} behavior on a per-test basis. {@code tester} object is created in the test object constructor, and there's no trivial way to override its features.
     * This JUnit rule enables post-process test object on a per test method basis
     * // note dl not sure if it was even used  before
     */
    public static class TesterConfigurationExtension implements ParameterResolver, TestExecutionExceptionHandler {

        @Override
        public void handleTestExecutionException( ExtensionContext context, Throwable throwable ) throws Throwable {
            throw throwable;
        }


        @Override
        public boolean supportsParameter( ParameterContext parameterContext, ExtensionContext extensionContext ) {
            return parameterContext.getParameter().getType().equals( SqlValidatorTestCase.class );
        }


        @Override
        public Object resolveParameter( ParameterContext parameterContext, ExtensionContext extensionContext ) {
            SqlValidatorTestCase testCase = (SqlValidatorTestCase) extensionContext.getTestInstance().orElseThrow();
            SqlTester tester = testCase.tester;

            Method testMethod = extensionContext.getRequiredTestMethod();
            WithLex lex = testMethod.getAnnotation( WithLex.class );

            if ( lex != null ) {
                tester = tester.withLex( lex.value() );
            }

            testCase.tester = tester;
            return testCase;
        }

    }


}
