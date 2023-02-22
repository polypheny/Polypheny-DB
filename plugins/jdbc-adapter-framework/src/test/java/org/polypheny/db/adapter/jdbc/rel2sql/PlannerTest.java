/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.jdbc.rel2sql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.adapter.jdbc.JdbcAlg;
import org.polypheny.db.adapter.jdbc.JdbcConvention;
import org.polypheny.db.adapter.jdbc.JdbcImplementor;
import org.polypheny.db.adapter.jdbc.JdbcRules;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.rules.FilterMergeRule;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.HrSchema;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.sql.util.PlannerImplMock;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.util.Util;

public class PlannerTest extends SqlLanguageDependent {

    /**
     * Unit test that calls {@link Planner#transform} twice, with different rule sets, with different conventions.
     *
     * {@link JdbcConvention} is different from the typical convention in that it is not a singleton. Switching to a different
     * instance causes problems unless planner state is wiped clean between calls to {@link Planner#transform}.
     */
    @Test
    public void testPlanTransformWithDiffRuleSetAndConvention() throws Exception {
        Program program0 = Programs.ofRules( FilterMergeRule.INSTANCE, EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_PROJECT_RULE );

        JdbcConvention out = new JdbcConvention( null, null, "myjdbc" );
        Program program1 = Programs.ofRules( new MockJdbcProjectRule( out ), new MockJdbcTableRule( out ) );

        Planner planner = getPlanner( null, program0, program1 );
        Node parse = planner.parse( "select T1.\"name\" from \"emps\" as T1 " );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();

        AlgTraitSet traitSet0 = convert.getTraitSet().replace( EnumerableConvention.INSTANCE );

        AlgTraitSet traitSet1 = convert.getTraitSet().replace( out );

        AlgNode transform = planner.transform( 0, traitSet0, convert );
        AlgNode transform2 = planner.transform( 1, traitSet1, transform );
        assertThat(
                toString( transform2 ),
                equalTo( "JdbcProject(model=[RELATIONAL], name=[$2])\n  MockJdbcScan(model=[RELATIONAL], table=[[hr, emps]])\n" ) );
    }


    private String toString( AlgNode alg ) {
        return Util.toLinux( AlgOptUtil.dumpPlan( "", alg, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
    }


    private Planner getPlanner( List<AlgTraitDef> traitDefs, Program... programs ) {
        return getPlanner( traitDefs, Parser.ParserConfig.DEFAULT, programs );
    }


    private Planner getPlanner( List<AlgTraitDef> traitDefs, ParserConfig parserConfig, Program... programs ) {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "hr", new ReflectiveSchema( new HrSchema(), -1 ), NamespaceType.RELATIONAL );

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
        return new PlannerImplMock( config );
    }


    /**
     * Rule to convert a {@link EnumerableProject} to an {@link JdbcRules.JdbcProject}.
     */
    public static class MockJdbcProjectRule extends ConverterRule {

        public MockJdbcProjectRule( JdbcConvention out ) {
            super( EnumerableProject.class, EnumerableConvention.INSTANCE, out, "MockJdbcProjectRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final EnumerableProject project = (EnumerableProject) alg;
            return new JdbcRules.JdbcProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( getOutConvention() ),
                    convert( project.getInput(), project.getInput().getTraitSet().replace( getOutConvention() ) ),
                    project.getProjects(),
                    project.getRowType() );
        }

    }


    /**
     * Rule to convert a {@link EnumerableScan} to an {@link MockJdbcScan}.
     */
    public static class MockJdbcTableRule extends ConverterRule {

        private MockJdbcTableRule( JdbcConvention out ) {
            super( EnumerableScan.class, EnumerableConvention.INSTANCE, out, "MockJdbcTableRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final EnumerableScan scan = (EnumerableScan) alg;
            return new PlannerTest.MockJdbcScan( scan.getCluster(), scan.getEntity(), (JdbcConvention) getOutConvention() );
        }

    }


    /**
     * Relational expression representing a "mock" scan of a table in a JDBC data source.
     */
    private static class MockJdbcScan extends Scan implements JdbcAlg {

        MockJdbcScan( AlgOptCluster cluster, AlgOptEntity table, JdbcConvention jdbcConvention ) {
            super( cluster, cluster.traitSetOf( jdbcConvention ), table );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new MockJdbcScan( getCluster(), table, (JdbcConvention) getConvention() );
        }


        @Override
        public void register( AlgOptPlanner planner ) {
            final JdbcConvention out = (JdbcConvention) getConvention();
            for ( AlgOptRule rule : JdbcRules.rules( out ) ) {
                planner.addRule( rule );
            }
        }


        @Override
        public JdbcImplementor.Result implement( JdbcImplementor implementor ) {
            return null;
        }

    }


}
