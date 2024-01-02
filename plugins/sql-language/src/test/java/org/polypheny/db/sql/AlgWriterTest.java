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

package org.polypheny.db.sql;


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.externalize.AlgJson;
import org.polypheny.db.algebra.externalize.AlgJsonReader;
import org.polypheny.db.algebra.externalize.AlgJsonWriter;


/**
 * Unit test for {@link AlgJson}.
 */
public class AlgWriterTest extends SqlLanguageDependent {

    private static final String XX = """
            {
              "Plan": {
                "id": "2",
                "relOp": "LogicalAggregate",
                "model": "RELATIONAL",
                "group": "{0}",
                "aggs": "[COUNT(DISTINCT deptno), COUNT()]",
                "rowcount": 1.5,
                "rows cost": 116.875,
                "cpu cost": 201.0,
                "io cost": 0.0,
                "inputs": [
                  {
                    "id": "1",
                    "relOp": "LogicalFilter",
                    "model": "RELATIONAL",
                    "condition": "=(deptno, 10)",
                    "rowcount": 15.0,
                    "rows cost": 115.0,
                    "cpu cost": 201.0,
                    "io cost": 0.0,
                    "inputs": [
                      {
                        "id": "0",
                        "relOp": "LogicalScan",
                        "model": "RELATIONAL",
                        "table": "[hr, emps]",
                        "rowcount": 100.0,
                        "rows cost": 100.0,
                        "cpu cost": 101.0,
                        "io cost": 0.0,
                        "inputs": []
                      }
                    ]
                  }
                ]
              }
            }""";


    /**
     * Unit test for {@link AlgJsonWriter} on a simple tree of relational expressions, consisting of a table, a filter and an aggregate node.
     */
    @Test
    public void testWriter() {
        /*String s =
                Frameworks.withPlanner( ( cluster, algOptSchema, rootSchema ) -> {
                    rootSchema.add( "hr", new ReflectiveSchema( new HrSchema(), -1 ), NamespaceType.RELATIONAL );
                    LogicalRelScan scan =
                            LogicalRelScan.create(
                                    cluster,
                                    //algOptSchema.getTableForMember( Arrays.asList( "hr", "emps" ) ) );
                    final RexBuilder rexBuilder = cluster.getRexBuilder();
                    LogicalFilter filter =
                            LogicalFilter.create(
                                    scan,
                                    rexBuilder.makeCall(
                                            OperatorRegistry.get( OperatorName.EQUALS ),
                                            rexBuilder.makeFieldAccess( rexBuilder.makeRangeReference( scan ), "deptno", true ),
                                            rexBuilder.makeExactLiteral( BigDecimal.TEN ) ) );
                    final AlgJsonWriter writer = new AlgJsonWriter();
                    final AlgDataType bigIntType = cluster.getTypeFactory().createPolyType( PolyType.BIGINT );
                    LogicalAggregate aggregate =
                            LogicalAggregate.create( filter, ImmutableBitSet.of( 0 ), null,
                                    ImmutableList.of(
                                            AggregateCall.create( OperatorRegistry.getAgg( OperatorName.COUNT ), true, false, ImmutableList.of( 1 ), -1, AlgCollations.EMPTY, bigIntType, "c" ),
                                            AggregateCall.create( OperatorRegistry.getAgg( OperatorName.COUNT ), false, false, ImmutableList.of(), -1, AlgCollations.EMPTY, bigIntType, "d" ) ) );
                    aggregate.explain( writer );
                    return writer.asString();
                } );
        assertThat( s, is( XX ) );*/
    }


    /**
     * Unit test for {@link AlgJsonReader}.
     */
    @Test
    @Disabled // TODO MV: The test if working if you put " around the table names in the JSON ( instead of \"table\": \"[hr, emps]\",\n" --> \"table\": \"[\"hr\", \"emps\"]\",\n" )
    public void testReader() {
        /*String s =
                Frameworks.withPlanner( ( cluster, algOptSchema, rootSchema ) -> {
                    rootSchema.add( "hr", new ReflectiveSchema( new HrSchema(), -1 ), NamespaceType.RELATIONAL );
                    final AlgJsonReader reader = new AlgJsonReader( cluster, algOptSchema, rootSchema );
                    AlgNode node;
                    try {
                        node = reader.read( XX );
                    } catch ( IOException e ) {
                        throw new RuntimeException( e );
                    }
                    return AlgOptUtil.dumpPlan( "", node, ExplainFormat.TEXT, ExplainLevel.EXPPLAN_ATTRIBUTES );
                } );

        assertThat(
                s,
                Matchers.isLinux( "LogicalAggregate(group=[{0}], agg#0=[COUNT(DISTINCT $1)], agg#1=[COUNT()])\n"
                        + "  LogicalFilter(condition=[=($1, 10)])\n"
                        + "    LogicalScan(table=[[hr, emps]])\n" ) );*/
    }

}

