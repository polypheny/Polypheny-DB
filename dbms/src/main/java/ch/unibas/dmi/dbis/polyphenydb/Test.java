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

package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvTable.Flavor;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.catalog.TablePrinter;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.Config;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunners;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;


public class Test {

    public static void main( String[] args ) throws SqlParseException, ValidationException, RelConversionException, SQLException {

        //final String sql = "INSERT INTO emps(empid,deptno,name,salary,commission) VALUES (100, 10, 'Bill', 10000, 1000)";
        //final String sql = "INSERT INTO depts(deptno,name) VALUES (40, 'IT')";

        final String sql = "SELECT * FROM csv.emps";
        //final String sql = "SELECT sum(salary) FROM csv.emps";
        //final String sql = "SELECT * FROM csv.emps WHERE csv.emps.empid > 110";
        //final String sql = "SELECT count(empid) as anz FROM csv.emps GROUP BY csv.emps.deptno";
        //final String sql = "SELECT * FROM csv.emps e, csv.depts d WHERE e.deptno = d.deptno";

        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( false );
        Config parserConfig = configConfigBuilder.build();

        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );

        // CSV
        File csvDir = new File( "testTestCsv" );
        rootSchema.add( "CSV", new CsvSchema( csvDir, Flavor.FILTERABLE ) );

        // JDBC
        final DataSource ds1 = JdbcSchema.dataSource( "jdbc:hsqldb:file:testdb", "org.hsqldb.jdbcDriver", "", "" );
        final JdbcSchema jdbcSchema = JdbcSchema.create( rootSchema, "DB1", ds1, null, null );
        //final Map<String, JdbcTable> tableMap = getJdbcTableMap(jdbcSchema);
        rootSchema.add( "HSQLDB", jdbcSchema );

        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder();
        SqlToRelConverter.Config sqlToRelConfig = sqlToRelConfigBuilder.build();

        List<RelTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        Frameworks.ConfigBuilder frameworkConfigBuilder = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .traitDefs( traitDefs )
                .defaultSchema( rootSchema )
                .sqlToRelConverterConfig( sqlToRelConfig )
                .programs( Programs.ofRules( Programs.RULE_SET ) );
        //.programs( Programs.ofRules( Programs.CALC_RULES ) );
        FrameworkConfig frameworkConfig = frameworkConfigBuilder.build();

        RelNode bestPlan = planQuery( frameworkConfig, sql, true );

        // Execute query
        ResultSet resultSet = null;
        try ( PreparedStatement preparedStatement = RelRunners.run( bestPlan ) ) {
            resultSet = preparedStatement.executeQuery();
            System.out.println( TablePrinter.processResultSet( resultSet ) );
        } finally {
            if ( resultSet != null && !resultSet.isClosed() ) {
                resultSet.close();
            }
        }

    }


    /*
    private static Map<String, JdbcTable> getJdbcTableMap( JdbcSchema jdbcSchema ) {
        final Map<String, JdbcTable> tableMap  = ImmutableMap.<String, JdbcTable>builder()
                .put( "test",
                        new JdbcTable(
                                jdbcSchema,
                                "DB",
                                "HSQLDB",
                                "test",
                                null
                        ) ).build();
        return tableMap;
    }*/


    private static RelNode planQuery( FrameworkConfig config, String query, boolean debug ) throws RelConversionException, SqlParseException, ValidationException {
        Planner planner = Frameworks.getPlanner( config );
        if ( debug ) {
            System.out.println( "Query:" + query );
        }
        SqlNode n = planner.parse( query );
        n = planner.validate( n );
        RelNode root = planner.rel( n ).project();
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Logical Plan", root, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        RelOptCluster cluster = root.getCluster();
        final RelOptPlanner optPlanner = cluster.getPlanner();

        RelTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
        final RelNode newRoot = optPlanner.changeTraits( root, desiredTraits );
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Mid Plan", newRoot, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        optPlanner.setRoot( newRoot );
        RelNode bestExp = optPlanner.findBestExp();
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Best Plan", bestExp, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        return bestExp;
    }


}
