/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.plan;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.externalize.RelJsonReader;
import org.polypheny.db.rel.externalize.RelJsonWriter;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.HrSchema;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.test.Matchers;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Unit test for {@link org.polypheny.db.rel.externalize.RelJson}.
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
                    rootSchema.add( "hr", new ReflectiveSchema( new HrSchema() ), SchemaType.RELATIONAL );
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
                    final RelDataType bigIntType = cluster.getTypeFactory().createPolyType( PolyType.BIGINT );
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
     * Unit test for {@link org.polypheny.db.rel.externalize.RelJsonReader}.
     */
    @Test
    @Ignore // TODO MV: The test if working if you put " around the table names in the JSON ( instead of \"table\": \"[hr, emps]\",\n" --> \"table\": \"[\"hr\", \"emps\"]\",\n" )
    public void testReader() {
        String s =
                Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
                    rootSchema.add( "hr", new ReflectiveSchema( new HrSchema() ), SchemaType.RELATIONAL );
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

