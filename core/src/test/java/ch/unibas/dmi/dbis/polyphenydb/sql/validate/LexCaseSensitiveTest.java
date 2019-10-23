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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableProject;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.HrSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.Lex;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.SqlParserConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.Program;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import java.util.List;
import org.junit.Test;


/**
 * Testing {@link SqlValidator} and {@link Lex}.
 */
public class LexCaseSensitiveTest {

    private static Planner getPlanner( List<RelTraitDef> traitDefs, SqlParserConfig parserConfig, Program... programs ) {
        final SchemaPlus schema = Frameworks.createRootSchema( true ).add( "hr", new ReflectiveSchema( new HrSchema() ) );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .defaultSchema( schema )
                .traitDefs( traitDefs )
                .programs( programs )
                .prepareContext( new ContextImpl( PolyphenyDbSchema.from( schema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build();
        return Frameworks.getPlanner( config );
    }


    private static void runProjectQueryWithLex( Lex lex, String sql ) throws SqlParseException, ValidationException, RelConversionException {
        boolean oldCaseSensitiveValue = RuntimeConfig.CASE_SENSITIVE.getBoolean();
        try {
            SqlParserConfig javaLex = SqlParser.configBuilder().setLex( lex ).build();
            RuntimeConfig.CASE_SENSITIVE.setBoolean( lex.caseSensitive );
            Planner planner = getPlanner( null, javaLex, Programs.ofRules( Programs.RULE_SET ) );
            SqlNode parse = planner.parse( sql );
            SqlNode validate = planner.validate( parse );
            RelNode convert = planner.rel( validate ).rel;
            RelTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
            RelNode transform = planner.transform( 0, traitSet, convert );
            assertThat( transform, instanceOf( EnumerableProject.class ) );
            List<String> fieldNames = transform.getRowType().getFieldNames();
            assertThat( fieldNames.size(), is( 2 ) );
            if ( lex.caseSensitive ) {
                assertThat( fieldNames.get( 0 ), is( "EMPID" ) );
                assertThat( fieldNames.get( 1 ), is( "empid" ) );
            } else {
                assertThat( fieldNames.get( 0 ) + "-" + fieldNames.get( 1 ), anyOf( is( "EMPID-empid0" ), is( "EMPID0-empid" ) ) );
            }
        } finally {
            RuntimeConfig.CASE_SENSITIVE.setBoolean( oldCaseSensitiveValue );
        }
    }


    @Test
    public void testPolyphenyDbCaseOracle() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select \"empid\" as EMPID, \"empid\" from\n (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
        runProjectQueryWithLex( Lex.ORACLE, sql );
    }


    @Test(expected = ValidationException.class)
    public void testPolyphenyDbCaseOracleException() throws SqlParseException, ValidationException, RelConversionException {
        // Oracle is case sensitive, so EMPID should not be found.
        String sql = "select EMPID, \"empid\" from\n (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
        runProjectQueryWithLex( Lex.ORACLE, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySql() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select empid as EMPID, empid from (\n select empid from emps order by `EMPS`.DEPTNO)";
        runProjectQueryWithLex( Lex.MYSQL, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySqlNoException() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.MYSQL, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySqlAnsi() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select empid as EMPID, empid from (\n select empid from emps order by EMPS.DEPTNO)";
        runProjectQueryWithLex( Lex.MYSQL_ANSI, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySqlAnsiNoException() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.MYSQL_ANSI, sql );
    }


    @Test
    public void testPolyphenyDbCaseSqlServer() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select empid as EMPID, empid from (\n  select empid from emps order by EMPS.DEPTNO)";
        runProjectQueryWithLex( Lex.SQL_SERVER, sql );
    }


    @Test
    public void testPolyphenyDbCaseSqlServerNoException() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.SQL_SERVER, sql );
    }


    @Test
    public void testPolyphenyDbCaseJava() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select empid as EMPID, empid from (\n  select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.JAVA, sql );
    }


    @Test(expected = ValidationException.class)
    public void testPolyphenyDbCaseJavaException() throws SqlParseException, ValidationException, RelConversionException {
        // JAVA is case sensitive, so EMPID should not be found.
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.JAVA, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinOracle() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select t.\"empid\" as EMPID, s.\"empid\" from\n"
                + "(select * from \"emps\" where \"emps\".\"deptno\" > 100) t join\n"
                + "(select * from \"emps\" where \"emps\".\"deptno\" < 200) s\n"
                + "on t.\"empid\" = s.\"empid\"";
        runProjectQueryWithLex( Lex.ORACLE, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinMySql() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.MYSQL, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinMySqlAnsi() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.MYSQL_ANSI, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinSqlServer() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.SQL_SERVER, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinJava() throws SqlParseException, ValidationException, RelConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.JAVA, sql );
    }
}
