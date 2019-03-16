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
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.test.JdbcTest.HrSchema;
import ch.unibas.dmi.dbis.polyphenydb.test.Matchers;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import org.junit.Test;


/**
 * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.rel.externalize.RelJson}.
 */
public class RelWriterTest {

    public static final String XX = "{\n"
            + "  \"rels\": [\n"
            + "    {\n"
            + "      \"id\": \"0\",\n"
            + "      \"relOp\": \"LogicalTableScan\",\n"
            + "      \"table\": [\n"
            + "        \"hr\",\n"
            + "        \"emps\"\n"
            + "      ],\n"
            + "      \"inputs\": []\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": \"1\",\n"
            + "      \"relOp\": \"LogicalFilter\",\n"
            + "      \"condition\": {\n"
            + "        \"op\": \"=\",\n"
            + "        \"operands\": [\n"
            + "          {\n"
            + "            \"input\": 1,\n"
            + "            \"name\": \"$1\"\n"
            + "          },\n"
            + "          10\n"
            + "        ]\n"
            + "      }\n"
            + "    },\n"
            + "    {\n"
            + "      \"id\": \"2\",\n"
            + "      \"relOp\": \"LogicalAggregate\",\n"
            + "      \"group\": [\n"
            + "        0\n"
            + "      ],\n"
            + "      \"aggs\": [\n"
            + "        {\n"
            + "          \"agg\": \"COUNT\",\n"
            + "          \"type\": {\n"
            + "            \"type\": \"BIGINT\",\n"
            + "            \"nullable\": false\n"
            + "          },\n"
            + "          \"distinct\": true,\n"
            + "          \"operands\": [\n"
            + "            1\n"
            + "          ]\n"
            + "        },\n"
            + "        {\n"
            + "          \"agg\": \"COUNT\",\n"
            + "          \"type\": {\n"
            + "            \"type\": \"BIGINT\",\n"
            + "            \"nullable\": false\n"
            + "          },\n"
            + "          \"distinct\": false,\n"
            + "          \"operands\": []\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
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
                    final RelDataType bigIntType =
                            cluster.getTypeFactory().createSqlType( SqlTypeName.BIGINT );
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
    public void testReader() {
        String s =
                Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
                    SchemaPlus schema = rootSchema.add( "hr", new ReflectiveSchema( new HrSchema() ) );
                    final RelJsonReader reader = new RelJsonReader( cluster, relOptSchema, schema );
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

