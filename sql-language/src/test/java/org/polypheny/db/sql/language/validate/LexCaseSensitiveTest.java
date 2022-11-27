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

package org.polypheny.db.sql.language.validate;


import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Lex;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.HrSchema;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.SqlLanguagelDependant;
import org.polypheny.db.tools.AlgConversionException;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.ValidationException;


/**
 * Testing {@link SqlValidator} and {@link Lex}.
 */
public class LexCaseSensitiveTest extends SqlLanguagelDependant {

    private static Planner getPlanner( List<AlgTraitDef> traitDefs, ParserConfig parserConfig, Program... programs ) {
        final SchemaPlus schema = Frameworks.createRootSchema( true ).add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .defaultSchema( schema )
                .traitDefs( traitDefs )
                .programs( programs )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( schema ),
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


    private static void runProjectQueryWithLex( Lex lex, String sql ) throws NodeParseException, ValidationException, AlgConversionException {
        boolean oldCaseSensitiveValue = RuntimeConfig.RELATIONAL_CASE_SENSITIVE.getBoolean();
        try {
            ParserConfig javaLex = Parser.configBuilder().setLex( lex ).build();
            RuntimeConfig.RELATIONAL_CASE_SENSITIVE.setBoolean( lex.caseSensitive );
            Planner planner = getPlanner( null, javaLex, Programs.ofRules( Programs.RULE_SET ) );
            Node parse = planner.parse( sql );
            Node validate = planner.validate( parse );
            AlgNode convert = planner.alg( validate ).alg;
            AlgTraitSet traitSet = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );
            AlgNode transform = planner.transform( 0, traitSet, convert );
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
            RuntimeConfig.RELATIONAL_CASE_SENSITIVE.setBoolean( oldCaseSensitiveValue );
        }
    }


    @Test
    public void testPolyphenyDbCaseOracle() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select \"empid\" as EMPID, \"empid\" from\n (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
        runProjectQueryWithLex( Lex.ORACLE, sql );
    }


    @Test(expected = ValidationException.class)
    public void testPolyphenyDbCaseOracleException() throws NodeParseException, ValidationException, AlgConversionException {
        // Oracle is case sensitive, so EMPID should not be found.
        String sql = "select EMPID, \"empid\" from\n (select \"empid\" from \"emps\" order by \"emps\".\"deptno\")";
        runProjectQueryWithLex( Lex.ORACLE, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySql() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select empid as EMPID, empid from (\n select empid from emps order by `EMPS`.DEPTNO)";
        runProjectQueryWithLex( Lex.MYSQL, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySqlNoException() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.MYSQL, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySqlAnsi() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select empid as EMPID, empid from (\n select empid from emps order by EMPS.DEPTNO)";
        runProjectQueryWithLex( Lex.MYSQL_ANSI, sql );
    }


    @Test
    public void testPolyphenyDbCaseMySqlAnsiNoException() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.MYSQL_ANSI, sql );
    }


    @Test
    public void testPolyphenyDbCaseSqlServer() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select empid as EMPID, empid from (\n  select empid from emps order by EMPS.DEPTNO)";
        runProjectQueryWithLex( Lex.SQL_SERVER, sql );
    }


    @Test
    public void testPolyphenyDbCaseSqlServerNoException() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.SQL_SERVER, sql );
    }


    @Test
    public void testPolyphenyDbCaseJava() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select empid as EMPID, empid from (\n  select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.JAVA, sql );
    }


    @Test(expected = ValidationException.class)
    public void testPolyphenyDbCaseJavaException() throws NodeParseException, ValidationException, AlgConversionException {
        // JAVA is case sensitive, so EMPID should not be found.
        String sql = "select EMPID, empid from\n (select empid from emps order by emps.deptno)";
        runProjectQueryWithLex( Lex.JAVA, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinOracle() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select t.\"empid\" as EMPID, s.\"empid\" from\n"
                + "(select * from \"emps\" where \"emps\".\"deptno\" > 100) t join\n"
                + "(select * from \"emps\" where \"emps\".\"deptno\" < 200) s\n"
                + "on t.\"empid\" = s.\"empid\"";
        runProjectQueryWithLex( Lex.ORACLE, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinMySql() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.MYSQL, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinMySqlAnsi() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.MYSQL_ANSI, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinSqlServer() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.SQL_SERVER, sql );
    }


    @Test
    public void testPolyphenyDbCaseJoinJava() throws NodeParseException, ValidationException, AlgConversionException {
        String sql = "select t.empid as EMPID, s.empid from\n"
                + "(select * from emps where emps.deptno > 100) t join\n"
                + "(select * from emps where emps.deptno < 200) s on t.empid = s.empid";
        runProjectQueryWithLex( Lex.JAVA, sql );
    }

}
