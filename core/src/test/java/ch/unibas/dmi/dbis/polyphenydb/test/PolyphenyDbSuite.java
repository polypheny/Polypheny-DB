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

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.adapter.clone.ArrayTableTest;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbRemoteEmbeddedDriverTest;
import ch.unibas.dmi.dbis.polyphenydb.materialize.LatticeSuggesterTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanReaderTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtilTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelWriterTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.CollationConversionTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.ComboRuleTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.TraitConversionTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.TraitPropagationTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.VolcanoPlannerTest;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.VolcanoPlannerTraitTest;
import ch.unibas.dmi.dbis.polyphenydb.prepare.LookupOperatorOverloadsTest;
import ch.unibas.dmi.dbis.polyphenydb.profile.ProfilerTest;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTest;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTest;
import ch.unibas.dmi.dbis.polyphenydb.rel.rel2sql.RelToSqlConverterTest;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.DateRangeRulesTest;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortRemoveRuleTest;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilderTest;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutorTest;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexSqlStandardConvertletTableTest;
import ch.unibas.dmi.dbis.polyphenydb.runtime.BinarySearchTest;
import ch.unibas.dmi.dbis.polyphenydb.runtime.EnumerablesTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSetOptionOperatorTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlUnParserTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.parserextensiontesting.ExtensionSqlParserTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlAdvisorTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlOperatorTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlPrettyWriterTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.test.SqlTypeNameTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtilTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.LexCaseSensitiveTest;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtilTest;
import ch.unibas.dmi.dbis.polyphenydb.test.enumerable.EnumerableCorrelateTest;
import ch.unibas.dmi.dbis.polyphenydb.test.fuzzer.RexProgramFuzzyTest;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworksTest;
import ch.unibas.dmi.dbis.polyphenydb.tools.PlannerTest;
import ch.unibas.dmi.dbis.polyphenydb.util.BitSetsTest;
import ch.unibas.dmi.dbis.polyphenydb.util.ChunkListTest;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSetTest;
import ch.unibas.dmi.dbis.polyphenydb.util.PartiallyOrderedSetTest;
import ch.unibas.dmi.dbis.polyphenydb.util.PermutationTestCase;
import ch.unibas.dmi.dbis.polyphenydb.util.PrecedenceClimbingParserTest;
import ch.unibas.dmi.dbis.polyphenydb.util.ReflectVisitorTest;
import ch.unibas.dmi.dbis.polyphenydb.util.SourceTest;
import ch.unibas.dmi.dbis.polyphenydb.util.TestUtilTest;
import ch.unibas.dmi.dbis.polyphenydb.util.UtilTest;
import ch.unibas.dmi.dbis.polyphenydb.util.graph.DirectedGraphTest;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.MappingTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


/**
 * Polypheny-DB test suite.
 *
 * Tests are sorted by approximate running time. The suite runs the fastest tests first, so that regressions can be discovered as fast as possible. Most unit tests run very quickly, and are scheduled before system tests (which are slower but more likely to break because
 * they have more dependencies). Slow unit tests that don't break often are scheduled last.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        // very fast tests (under 0.1s)
        ArrayTableTest.class,
        BitSetsTest.class,
        ImmutableBitSetTest.class,
        DirectedGraphTest.class,
        ReflectVisitorTest.class,
        RelOptUtilTest.class,
        RelCollationTest.class,
        UtilTest.class,
        PrecedenceClimbingParserTest.class,
        SourceTest.class,
        MappingTest.class,
        PolyphenyDbResourceTest.class,
        FilteratorTest.class,
        PermutationTestCase.class,
        SqlFunctionsTest.class,
        SqlJsonFunctionsTest.class,
        SqlTypeNameTest.class,
        ModelTest.class,
        SqlValidatorFeatureTest.class,
        VolcanoPlannerTraitTest.class,
        InterpreterTest.class,
        TestUtilTest.class,
        VolcanoPlannerTest.class,
        RelTraitTest.class,
        HepPlannerTest.class,
        TraitPropagationTest.class,
        RelDistributionTest.class,
        RelWriterTest.class,
        RexProgramTest.class,
        SqlOperatorBindingTest.class,
        RexTransformerTest.class,
        BinarySearchTest.class,
        EnumerablesTest.class,
        ExceptionMessageTest.class,
        InduceGroupingTypeTest.class,
        RelOptPlanReaderTest.class,
        RexBuilderTest.class,
        RexSqlStandardConvertletTableTest.class,
        SqlTypeFactoryTest.class,
        SqlTypeUtilTest.class,
        SqlValidatorUtilTest.class,

        // medium tests (above 0.1s)
        SqlParserTest.class,
        SqlUnParserTest.class,
        ExtensionSqlParserTest.class,
        SqlSetOptionOperatorTest.class,
        SqlPrettyWriterTest.class,
        SqlValidatorTest.class,
        SqlValidatorDynamicTest.class,
        SqlValidatorMatchTest.class,
        SqlAdvisorTest.class,
        RelMetadataTest.class,
        DateRangeRulesTest.class,
        ScannableTableTest.class,
        RexExecutorTest.class,
        SqlLimitsTest.class,
        JdbcFrontLinqBackTest.class,
        RelToSqlConverterTest.class,
        SqlOperatorTest.class,
        ChunkListTest.class,
        FrameworksTest.class,
        EnumerableCorrelateTest.class,
        LookupOperatorOverloadsTest.class,
        LexCaseSensitiveTest.class,
        CollationConversionTest.class,
        TraitConversionTest.class,
        ComboRuleTest.class,
        MutableRelTest.class,

        // slow tests (above 1s)
        UdfTest.class,
        UdtTest.class,
        TableFunctionTest.class,
        PlannerTest.class,
        RelBuilderTest.class,
        PigRelBuilderTest.class,
        RexImplicationCheckerTest.class,
        JdbcAdapterTest.class,
        LinqFrontJdbcBackTest.class,
        JdbcFrontJdbcBackLinqMiddleTest.class,
        RexProgramFuzzyTest.class,
        SqlToRelConverterTest.class,
        ProfilerTest.class,
        SqlAdvisorJdbcTest.class,
        CoreQuidemTest.class,
        PolyphenyDbRemoteEmbeddedDriverTest.class,
        StreamTest.class,
        SortRemoveRuleTest.class,

        // above 10sec
        JdbcFrontJdbcBackTest.class,

        // above 20sec
        JdbcTest.class,
        PolyphenyDbSqlOperatorTest.class,
        ReflectiveSchemaTest.class,
        RelOptRulesTest.class,

        // test cases
        TableInRootSchemaTest.class,
        RelMdColumnOriginsTest.class,
        MultiJdbcSchemaJoinTest.class,
        CollectionTypeTest.class,

        // slow tests that don't break often
        SqlToRelConverterExtendedTest.class,
        PartiallyOrderedSetTest.class,

        // above 30sec
        LatticeSuggesterTest.class,
        MaterializationTest.class,

        // above 120sec
        LatticeTest.class,

        // system tests and benchmarks (very slow, but usually only run if
        // '-Dpolyphenydb.test.slow' is specified)
        FoodmartTest.class
})
public class PolyphenyDbSuite {

}
