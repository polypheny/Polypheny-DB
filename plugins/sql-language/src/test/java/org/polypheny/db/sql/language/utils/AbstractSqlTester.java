/*
 * Copyright 2019-2022 The Polypheny Project
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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.constant.Lex;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIntervalLiteral;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlTestFactory;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.dialect.AnsiSqlDialect;
import org.polypheny.db.sql.language.parser.SqlParserUtil;
import org.polypheny.db.sql.language.parser.SqlParserUtil.StringAndPos;
import org.polypheny.db.sql.language.util.SqlShuttle;
import org.polypheny.db.sql.language.utils.SqlValidatorTestCase.Tester;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorNamespace;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Collation.Coercibility;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TestUtil;
import org.polypheny.db.util.Util;


/**
 * Abstract implementation of {@link Tester} that talks to a mock catalog.
 *
 * This is to implement the default behavior: testing is only against the {@link SqlValidator}.
 */
public abstract class AbstractSqlTester implements SqlTester, AutoCloseable {

    protected final SqlTestFactory factory;


    public AbstractSqlTester( SqlTestFactory factory ) {
        this.factory = factory;
    }


    @Override
    public final SqlTestFactory getFactory() {
        return factory;
    }


    /**
     * {@inheritDoc}
     *
     * This default implementation does nothing.
     */
    @Override
    public void close() {
        // no resources to release
    }


    @Override
    public final Conformance getConformance() {
        return (Conformance) factory.get( "conformance" );
    }


    @Override
    public final SqlValidator getValidator() {
        return factory.getValidator();
    }


    @Override
    public void assertExceptionIsThrown( String sql, String expectedMsgPattern ) {
        final SqlValidator validator;
        final SqlNode sqlNode;
        final StringAndPos sap = SqlParserUtil.findPos( sql );
        try {
            sqlNode = parseQuery( sap.sql );
            validator = getValidator();
        } catch ( Throwable e ) {
            checkParseEx( e, expectedMsgPattern, sap.sql );
            return;
        }

        Throwable thrown = null;
        try {
            validator.validateSql( sqlNode );
        } catch ( Throwable ex ) {
            thrown = ex;
        }

        SqlValidatorTestCase.checkEx( thrown, expectedMsgPattern, sap );
    }


    protected void checkParseEx( Throwable e, String expectedMsgPattern, String sql ) {
        try {
            throw e;
        } catch ( NodeParseException spe ) {
            String errMessage = spe.getMessage();
            if ( expectedMsgPattern == null ) {
                throw new RuntimeException( "Error while parsing query:" + sql, spe );
            } else if ( errMessage == null || !errMessage.matches( expectedMsgPattern ) ) {
                throw new RuntimeException( "Error did not match expected [" + expectedMsgPattern + "] while parsing query [" + sql + "]", spe );
            }
        } catch ( Throwable t ) {
            throw new RuntimeException( "Error while parsing query: " + sql, t );
        }
    }


    @Override
    public AlgDataType getColumnType( String sql ) {
        AlgDataType rowType = getResultType( sql );
        final List<AlgDataTypeField> fields = rowType.getFieldList();
        assertEquals( "expected query to return 1 field", 1, fields.size() );
        return fields.get( 0 ).getType();
    }


    @Override
    public AlgDataType getResultType( String sql ) {
        SqlValidator validator = getValidator();
        SqlNode n = parseAndValidate( validator, sql );

        return validator.getValidatedNodeType( n );
    }


    @Override
    public SqlNode parseAndValidate( SqlValidator validator, String sql ) {
        if ( validator == null ) {
            validator = getValidator();
        }
        SqlNode sqlNode;
        try {
            sqlNode = parseQuery( sql );
        } catch ( Throwable e ) {
            throw new RuntimeException( "Error while parsing query: " + sql, e );
        }
        return validator.validateSql( sqlNode );
    }


    @Override
    public SqlNode parseQuery( String sql ) throws NodeParseException {
        Parser parser = factory.createParser( sql );
        return (SqlNode) parser.parseQuery();
    }


    @Override
    public void checkColumnType( String sql, String expected ) {
        AlgDataType actualType = getColumnType( sql );
        String actual = SqlTests.getTypeString( actualType );
        assertEquals( expected, actual );
    }


    @Override
    public void checkFieldOrigin( String sql, String fieldOriginList ) {
        SqlValidator validator = getValidator();
        SqlNode n = parseAndValidate( validator, sql );
        final List<List<String>> list = validator.getFieldOrigins( n );
        final StringBuilder buf = new StringBuilder( "{" );
        int i = 0;
        for ( List<String> strings : list ) {
            if ( i++ > 0 ) {
                buf.append( ", " );
            }
            if ( strings == null ) {
                buf.append( "null" );
            } else {
                int j = 0;
                for ( String s : strings ) {
                    if ( j++ > 0 ) {
                        buf.append( '.' );
                    }
                    buf.append( s );
                }
            }
        }
        buf.append( "}" );
        assertEquals( fieldOriginList, buf.toString() );
    }


    @Override
    public void checkResultType( String sql, String expected ) {
        AlgDataType actualType = getResultType( sql );
        String actual = SqlTests.getTypeString( actualType );
        assertEquals( expected, actual );
    }


    @Override
    public void checkIntervalConv( String sql, String expected ) {
        SqlValidator validator = getValidator();
        final SqlCall n = (SqlCall) parseAndValidate( validator, sql );

        SqlNode node = null;
        for ( int i = 0; i < n.operandCount(); i++ ) {
            node = SqlUtil.stripAs( n.operand( i ) );
            if ( node instanceof SqlCall ) {
                node = ((SqlCall) node).operand( 0 );
                break;
            }
        }

        assertNotNull( node );
        SqlIntervalLiteral intervalLiteral = (SqlIntervalLiteral) node;
        SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
        long l = interval.getIntervalQualifier().isYearMonth()
                ? SqlParserUtil.intervalToMonths( interval )
                : SqlParserUtil.intervalToMillis( interval );
        String actual = l + "";
        assertEquals( expected, actual );
    }


    @Override
    public void checkType( String expression, String type ) {
        for ( String sql : buildQueries( expression ) ) {
            checkColumnType( sql, type );
        }
    }


    @Override
    public void checkCollation( String expression, String expectedCollationName, Coercibility expectedCoercibility ) {
        for ( String sql : buildQueries( expression ) ) {
            AlgDataType actualType = getColumnType( sql );
            Collation collation = actualType.getCollation();

            assertEquals( expectedCollationName, collation.getCollationName() );
            assertEquals( expectedCoercibility, collation.getCoercibility() );
        }
    }


    @Override
    public void checkCharset( String expression, Charset expectedCharset ) {
        for ( String sql : buildQueries( expression ) ) {
            AlgDataType actualType = getColumnType( sql );
            Charset actualCharset = actualType.getCharset();

            if ( !expectedCharset.equals( actualCharset ) ) {
                fail( "\n" + "Expected=" + expectedCharset.name() + "\n" + "  actual=" + actualCharset.name() );
            }
        }
    }


    @Override
    public SqlTester withQuoting( Quoting quoting ) {
        return with( "quoting", quoting );
    }


    @Override
    public SqlTester withQuotedCasing( Casing casing ) {
        return with( "quotedCasing", casing );
    }


    @Override
    public SqlTester withUnquotedCasing( Casing casing ) {
        return with( "unquotedCasing", casing );
    }


    @Override
    public SqlTester withCaseSensitive( boolean sensitive ) {
        return with( "caseSensitive", sensitive );
    }


    @Override
    public SqlTester withLex( Lex lex ) {
        return withQuoting( lex.quoting )
                .withCaseSensitive( lex.caseSensitive )
                .withQuotedCasing( lex.quotedCasing )
                .withUnquotedCasing( lex.unquotedCasing );
    }


    @Override
    public SqlTester withConformance( Conformance conformance ) {
        if ( conformance == null ) {
            conformance = ConformanceEnum.DEFAULT;
        }
        final SqlTester tester = with( "conformance", conformance );
        // TODO MV: Fix
//        if ( conformance instanceof SqlConformanceEnum ) {
//            return tester.withConnectionFactory( PolyphenyDbAssert.EMPTY_CONNECTION_FACTORY.with( PolyphenyDbConnectionProperty.CONFORMANCE, conformance ) );
//        } else {
        return tester;
//        }
    }


    @Override
    public SqlTester withOperatorTable( OperatorTable operatorTable ) {
        return with( "operatorTable", operatorTable );
    }


    /*
    @Override
    public SqlTester withConnectionFactory( PolyphenyDbAssert.ConnectionFactory connectionFactory ) {
        return with( "connectionFactory", connectionFactory );
    } */


    protected final SqlTester with( final String name, final Object value ) {
        return with( factory.with( name, value ) );
    }


    protected abstract SqlTester with( SqlTestFactory factory );

    // SqlTester methods


    @Override
    public void setFor( Operator operator, VmName... unimplementedVmNames ) {
        // do nothing
    }


    @Override
    public void checkAgg( String expr, String[] inputValues, Object result, double delta ) {
        String query = SqlTests.generateAggQuery( expr, inputValues );
        check( query, SqlTests.ANY_TYPE_CHECKER, result, delta );
    }


    @Override
    public void checkAggWithMultipleArgs( String expr, String[][] inputValues, Object result, double delta ) {
        String query = SqlTests.generateAggQueryWithMultipleArgs( expr, inputValues );
        check( query, SqlTests.ANY_TYPE_CHECKER, result, delta );
    }


    @Override
    public void checkWinAgg( String expr, String[] inputValues, String windowSpec, String type, Object result, double delta ) {
        String query = SqlTests.generateWinAggQuery( expr, windowSpec, inputValues );
        check( query, SqlTests.ANY_TYPE_CHECKER, result, delta );
    }


    @Override
    public void checkScalar( String expression, Object result, String resultType ) {
        checkType( expression, resultType );
        for ( String sql : buildQueries( expression ) ) {
            check( sql, SqlTests.ANY_TYPE_CHECKER, result, 0 );
        }
    }


    @Override
    public void checkScalarExact( String expression, String result ) {
        for ( String sql : buildQueries( expression ) ) {
            check( sql, SqlTests.INTEGER_TYPE_CHECKER, result, 0 );
        }
    }


    @Override
    public void checkScalarExact( String expression, String expectedType, String result ) {
        for ( String sql : buildQueries( expression ) ) {
            TypeChecker typeChecker = new SqlTests.StringTypeChecker( expectedType );
            check( sql, typeChecker, result, 0 );
        }
    }


    @Override
    public void checkScalarApprox( String expression, String expectedType, double expectedResult, double delta ) {
        for ( String sql : buildQueries( expression ) ) {
            TypeChecker typeChecker = new SqlTests.StringTypeChecker( expectedType );
            check( sql, typeChecker, expectedResult, delta );
        }
    }


    @Override
    public void checkBoolean( String expression, Boolean result ) {
        for ( String sql : buildQueries( expression ) ) {
            if ( null == result ) {
                checkNull( expression );
            } else {
                check( sql, SqlTests.BOOLEAN_TYPE_CHECKER, result.toString(), 0 );
            }
        }
    }


    @Override
    public void checkString( String expression, String result, String expectedType ) {
        for ( String sql : buildQueries( expression ) ) {
            TypeChecker typeChecker = new SqlTests.StringTypeChecker( expectedType );
            check( sql, typeChecker, result, 0 );
        }
    }


    @Override
    public void checkNull( String expression ) {
        for ( String sql : buildQueries( expression ) ) {
            check( sql, SqlTests.ANY_TYPE_CHECKER, null, 0 );
        }
    }


    @Override
    public final void check( String query, TypeChecker typeChecker, Object result, double delta ) {
        check( query, typeChecker, SqlTests.ANY_PARAMETER_CHECKER, SqlTests.createChecker( result, delta ) );
    }


    @Override
    public void check( String query, TypeChecker typeChecker, ParameterChecker parameterChecker, ResultChecker resultChecker ) {
        // This implementation does NOT check the result! All it does is check the return type.

        if ( typeChecker == null ) {
            // Parse and validate. There should be no errors.
            Util.discard( getResultType( query ) );
        } else {
            // Parse and validate. There should be no errors. There must be 1 column. Get its type.
            AlgDataType actualType = getColumnType( query );

            // Check result type.
            typeChecker.checkType( actualType );
        }

        SqlValidator validator = getValidator();
        SqlNode n = parseAndValidate( validator, query );
        final AlgDataType parameterRowType = validator.getParameterRowType( n );
        parameterChecker.checkParameters( parameterRowType );
    }


    @Override
    public void checkMonotonic( String query, Monotonicity expectedMonotonicity ) {
        SqlValidator validator = getValidator();
        SqlNode n = parseAndValidate( validator, query );
        final AlgDataType rowType = validator.getValidatedNodeType( n );
        final SqlValidatorNamespace selectNamespace = validator.getSqlNamespace( n );
        final String field0 = rowType.getFieldList().get( 0 ).getName();
        final Monotonicity monotonicity = selectNamespace.getMonotonicity( field0 );
        assertThat( monotonicity, equalTo( expectedMonotonicity ) );
    }


    @Override
    public void checkRewrite( SqlValidator validator, String query, String expectedRewrite ) {
        SqlNode rewrittenNode = parseAndValidate( validator, query );
        String actualRewrite = rewrittenNode.toSqlString( AnsiSqlDialect.DEFAULT, false ).getSql();
        TestUtil.assertEqualsVerbose( expectedRewrite, Util.toLinux( actualRewrite ) );
    }


    @Override
    public void checkFails( String expression, String expectedError, boolean runtime ) {
        if ( runtime ) {
            // We need to test that the expression fails at runtime. Ironically, that means that it must succeed at prepare time.
            SqlValidator validator = getValidator();
            final String sql = buildQuery( expression );
            SqlNode n = parseAndValidate( validator, sql );
            assertNotNull( n );
        } else {
            checkQueryFails( buildQuery( expression ), expectedError );
        }
    }


    @Override
    public void checkQueryFails( String sql, String expectedError ) {
        assertExceptionIsThrown( sql, expectedError );
    }


    @Override
    public void checkQuery( String sql ) {
        assertExceptionIsThrown( sql, null );
    }


    @Override
    public Monotonicity getMonotonicity( String sql ) {
        final SqlValidator validator = getValidator();
        final SqlNode node = parseAndValidate( validator, sql );
        final SqlSelect select = (SqlSelect) node;
        final SqlNode selectItem0 = select.getSqlSelectList().getSqlList().get( 0 );
        final SqlValidatorScope scope = validator.getSelectScope( select );
        return selectItem0.getMonotonicity( scope );
    }


    public static String buildQuery( String expression ) {
        return "values (" + expression + ")";
    }


    public static String buildQueryAgg( String expression ) {
        return "select " + expression + " from (values (1)) as t(x) group by x";
    }


    /**
     * Builds a query that extracts all literals as columns in an underlying select.
     *
     * For example,
     *
     * {@code 1 < 5}
     *
     * becomes
     *
     * {@code SELECT p0 < p1 FROM (VALUES (1, 5)) AS t(p0, p1)}
     *
     * Null literals don't have enough type information to be extracted. We push down {@code CAST(NULL AS type)} but raw nulls such as {@code CASE 1 WHEN 2 THEN 'a' ELSE NULL END} are left as is.
     *
     * @param expression Scalar expression
     * @return Query that evaluates a scalar expression
     */
    protected String buildQuery2( String expression ) {
        // "values (1 < 5)"
        // becomes
        // "select p0 < p1 from (values (1, 5)) as t(p0, p1)"
        SqlNode x;
        final String sql = "values (" + expression + ")";
        try {
            x = parseQuery( sql );
        } catch ( NodeParseException e ) {
            throw new RuntimeException( e );
        }
        final Collection<SqlNode> literalSet = new LinkedHashSet<>();
        x.accept(
                new SqlShuttle() {
                    private final List<Operator> ops = ImmutableList.of(
                            OperatorRegistry.get( OperatorName.LITERAL_CHAIN ),
                            OperatorRegistry.get( OperatorName.LOCALTIME ),
                            OperatorRegistry.get( OperatorName.LOCALTIMESTAMP ),
                            OperatorRegistry.get( OperatorName.CURRENT_TIME ),
                            OperatorRegistry.get( OperatorName.CURRENT_TIMESTAMP ) );


                    @Override
                    public SqlNode visit( Literal literal ) {
                        if ( !isNull( literal ) && literal.getTypeName() != PolyType.SYMBOL ) {
                            literalSet.add( (SqlNode) literal );
                        }
                        return (SqlNode) literal;
                    }


                    @Override
                    public SqlNode visit( Call call ) {
                        final SqlOperator operator = (SqlOperator) call.getOperator();
                        if ( operator.getOperatorName() == OperatorName.CAST && isNull( call.operand( 0 ) ) ) {
                            literalSet.add( (SqlNode) call );
                            return (SqlNode) call;
                        } else if ( ops.contains( operator ) ) {
                            // "Argument to function 'LOCALTIME' must be a literal"
                            return (SqlNode) call;
                        } else {
                            return super.visit( call );
                        }
                    }


                    private boolean isNull( Node sqlNode ) {
                        return sqlNode instanceof SqlLiteral &&
                                ((SqlLiteral) sqlNode).getTypeName() == PolyType.NULL;
                    }
                } );
        final List<SqlNode> nodes = new ArrayList<>( literalSet );
        nodes.sort( ( o1, o2 ) -> {
            final ParserPos pos0 = o1.getPos();
            final ParserPos pos1 = o2.getPos();
            int c = -Utilities.compare( pos0.getLineNum(), pos1.getLineNum() );
            if ( c != 0 ) {
                return c;
            }
            return -Utilities.compare( pos0.getColumnNum(), pos1.getColumnNum() );
        } );
        String sql2 = sql;
        final List<Pair<String, String>> values = new ArrayList<>();
        int p = 0;
        for ( SqlNode literal : nodes ) {
            final ParserPos pos = literal.getPos();
            final int start = SqlParserUtil.lineColToIndex( sql, pos.getLineNum(), pos.getColumnNum() );
            final int end = SqlParserUtil.lineColToIndex( sql, pos.getEndLineNum(), pos.getEndColumnNum() ) + 1;
            String param = "p" + (p++);
            values.add( Pair.of( sql2.substring( start, end ), param ) );
            sql2 = sql2.substring( 0, start ) + param + sql2.substring( end );
        }
        if ( values.isEmpty() ) {
            values.add( Pair.of( "1", "p0" ) );
        }
        return "select " + sql2.substring( "values (".length(), sql2.length() - 1 ) + " from (values (" + Util.commaList( Pair.left( values ) ) + ")) as t(" + Util.commaList( Pair.right( values ) ) + ")";
    }


    /**
     * Converts a scalar expression into a list of SQL queries that evaluate it.
     *
     * @param expression Scalar expression
     * @return List of queries that evaluate an expression
     */
    private Iterable<String> buildQueries( final String expression ) {
        // Why an explicit iterable rather than a list? If there is a syntax error in the expression, the calling code discovers it before we try to parse it to do substitutions on the parse tree.
        return () -> new Iterator<String>() {
            int i = 0;


            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }


            @Override
            public String next() {
                switch ( i++ ) {
                    case 0:
                        return buildQuery( expression );
                    case 1:
                        return buildQuery2( expression );
                    default:
                        throw new NoSuchElementException();
                }
            }


            @Override
            public boolean hasNext() {
                return i < 2;
            }
        };
    }

}