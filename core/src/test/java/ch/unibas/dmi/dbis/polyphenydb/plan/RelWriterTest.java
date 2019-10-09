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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJsonReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJsonWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.schema.HrSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.test.Matchers;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJson}.
 */
public class RelWriterTest {

    private static final String XX = "{\n"
            + "  \"Plan\": {\n"
            + "    \"id\": \"2\",\n"
            + "    \"relOp\": \"LogicalAggregate\",\n"
            + "    \"group\": \"{0}\",\n"
            + "    \"aggs\": \"[COUNT(DISTINCT deptno), COUNT()]\",\n"
            + "    \"rowcount\": 1.5,\n"
            + "    \"rows cost\": 116.875,\n"
            + "    \"cpu cost\": 201.0,\n"
            + "    \"io cost\": 0.0,\n"
            + "    \"inputs\": [\n"
            + "      {\n"
            + "        \"id\": \"1\",\n"
            + "        \"relOp\": \"LogicalFilter\",\n"
            + "        \"condition\": \"=(deptno, 10)\",\n"
            + "        \"rowcount\": 15.0,\n"
            + "        \"rows cost\": 115.0,\n"
            + "        \"cpu cost\": 201.0,\n"
            + "        \"io cost\": 0.0,\n"
            + "        \"inputs\": [\n"
            + "          {\n"
            + "            \"id\": \"0\",\n"
            + "            \"relOp\": \"LogicalTableScan\",\n"
            + "            \"table\": \"[hr, emps]\",\n"
            + "            \"rowcount\": 100.0,\n"
            + "            \"rows cost\": 100.0,\n"
            + "            \"cpu cost\": 101.0,\n"
            + "            \"io cost\": 0.0,\n"
            + "            \"inputs\": []\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";


    /**
     * Unit test for {@link RelJsonWriter} on a simple tree of relational expressions, consisting of a table, a filter and an aggregate node.
     */
    @Test
    public void testWriter() {
        String s =
                Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
                    rootSchema.add( "hr", new ReflectiveSchema( new HrSchema() ) );
                    LogicalTableScan scan =
                            LogicalTableScan.create( cluster,
                                    relOptSchema.getTableForMember( Arrays.asList( "hr", "emps" ) ) );
                    final RexBuilder rexBuilder = cluster.getRexBuilder();
                    LogicalFilter filter =
                            LogicalFilter.create( scan,
                                    rexBuilder.makeCall(
                                            SqlStdOperatorTable.EQUALS,
                                            rexBuilder.makeFieldAccess( rexBuilder.makeRangeReference( scan ), "deptno", true ),
                                            rexBuilder.makeExactLiteral( BigDecimal.TEN ) ) );
                    final RelJsonWriter writer = new RelJsonWriter();
                    final RelDataType bigIntType = cluster.getTypeFactory().createSqlType( SqlTypeName.BIGINT );
                    LogicalAggregate aggregate =
                            LogicalAggregate.create( filter, ImmutableBitSet.of( 0 ), null,
                                    ImmutableList.of(
                                            AggregateCall.create( SqlStdOperatorTable.COUNT, true, false, ImmutableList.of( 1 ), -1, RelCollations.EMPTY, bigIntType, "c" ),
                                            AggregateCall.create( SqlStdOperatorTable.COUNT, false, false, ImmutableList.of(), -1, RelCollations.EMPTY, bigIntType, "d" ) ) );
                    aggregate.explain( writer );
                    return writer.asString();
                } );
        assertThat( s, is( XX ) );
    }


    /**
     * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJsonReader}.
     */
    @Test
    @Ignore // TODO MV: The test if working if you put " around the table names in the JSON ( instead of \"table\": \"[hr, emps]\",\n" --> \"table\": \"[\"hr\", \"emps\"]\",\n" )
    public void testReader() {
        String s =
                Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
                    rootSchema.add( "hr", new ReflectiveSchema( new HrSchema() ) );
                    final RelJsonReader reader = new RelJsonReader( cluster, relOptSchema, rootSchema );
                    RelNode node;
                    try {
                        node = reader.read( XX );
                    } catch ( IOException e ) {
                        throw new RuntimeException( e );
                    }
                    return RelOptUtil.dumpPlan( "", node, SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES );
                } );

        assertThat( s,
                Matchers.isLinux( "LogicalAggregate(group=[{0}], agg#0=[COUNT(DISTINCT $1)], agg#1=[COUNT()])\n"
                        + "  LogicalFilter(condition=[=($1, 10)])\n"
                        + "    LogicalTableScan(table=[[hr, emps]])\n" ) );
    }
}

