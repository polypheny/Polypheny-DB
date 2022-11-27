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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.adapter.geode.algebra;


import org.junit.Ignore;


/**
 * Tests based on {@code zips-min.json} dataset. Runs automatically as part of CI.
 */
@Ignore
public class GeodeZipsTest extends AbstractGeodeTest {

    // TODO MV: enable
//    @BeforeClass
//    public static void setUp() throws Exception {
//        Cache cache = POLICY.cache();
//        Region<?, ?> region = cache.<String, Object>createRegionFactory().create( "zips" );
//        new JsonLoader( region ).loadClasspathResource( "/zips-mini.json" );
//    }
//
//
//    private ConnectionFactory newConnectionFactory() {
//        return new ConnectionFactory() {
//            @Override
//            public Connection createConnection() throws SQLException {
//                final Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:lex=JAVA" );
//                final SchemaPlus root = connection.unwrap( PolyphenyDbEmbeddedConnection.class ).getRootSchema();
//
//                root.add( "geode", new GeodeSchema( POLICY.cache(), Collections.singleton( "zips" ) ) );
//
//                // add Polypheny-DB view programmatically
//                final String viewSql = "select \"_id\" AS \"id\", \"city\", \"loc\", "
//                        + "cast(\"pop\" AS integer) AS \"pop\", cast(\"state\" AS varchar(2)) AS \"state\" "
//                        + "from \"geode\".\"zips\"";
//
//                ViewTableMacro macro = ViewTable.viewMacro( root, viewSql, Collections.singletonList( "geode" ), Arrays.asList( "geode", "view" ), false );
//                root.add( "view", macro );
//
//                return connection;
//            }
//        };
//    }
//
//
//    private AssertThat polyphenyDbAssert() {
//        return PolyphenyDbAssert.that().with( newConnectionFactory() );
//    }
//
//
//    @Test
//    public void testGroupByView() {
//        polyphenyDbAssert()
//                .query( "SELECT state, SUM(pop) FROM view GROUP BY state" )
//                .returnsCount( 51 )
//                .queryContains( GeodeAssertions.query( "SELECT state AS state, SUM(pop) AS EXPR$1 FROM /zips GROUP BY state" ) );
//    }
//
//
//    @Test
//    @Ignore("Currently fails")
//    public void testGroupByViewWithAliases() {
//        polyphenyDbAssert()
//                .query( "SELECT state as st, SUM(pop) po FROM view GROUP BY state" )
//                .queryContains( GeodeAssertions.query( "SELECT state, SUM(pop) AS po FROM /zips GROUP BY state" ) )
//                .returnsCount( 51 )
//                .explainContains( "PLAN=GeodeToEnumerableConverter\n"
//                        + "  GeodeAggregate(group=[{1}], po=[SUM($0)])\n"
//                        + "    GeodeProject(pop=[CAST($3):INTEGER], state=[CAST($4):VARCHAR(2) CHARACTER SET"
//                        + " \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
//                        + "      GeodeScan(table=[[geode, zips]])\n" );
//    }
//
//
//    @Test
//    public void testGroupByRaw() {
//        polyphenyDbAssert()
//                .query( "SELECT state as st, SUM(pop) po FROM geode.zips GROUP BY state" )
//                .returnsCount( 51 )
//                .explainContains( "PLAN=GeodeToEnumerableConverter\n"
//                        + "  GeodeAggregate(group=[{4}], po=[SUM($3)])\n"
//                        + "    GeodeScan(table=[[geode, zips]])\n" );
//    }
//
//
//    @Test
//    public void testGroupByRawWithAliases() {
//        polyphenyDbAssert()
//                .query( "SELECT state AS st, SUM(pop) AS po FROM geode.zips GROUP BY state" )
//                .returnsCount( 51 )
//                .explainContains( "PLAN=GeodeToEnumerableConverter\n"
//                        + "  GeodeAggregate(group=[{4}], po=[SUM($3)])\n"
//                        + "    GeodeScan(table=[[geode, zips]])\n" );
//    }
//
//
//    @Test
//    public void testMaxRaw() {
//        polyphenyDbAssert()
//                .query( "SELECT MAX(pop) FROM view" )
//                .returns( "EXPR$0=112047\n" )
//                .queryContains( GeodeAssertions.query( "SELECT MAX(pop) AS EXPR$0 FROM /zips" ) );
//    }
//
//
//    @Test
//    @Ignore("Currently fails")
//    public void testJoin() {
//        polyphenyDbAssert()
//                .query( "SELECT r._id FROM geode.zips AS v JOIN geode.zips AS r ON v._id = r._id LIMIT 1" )
//                .returnsCount( 1 )
//                .explainContains( "PLAN=EnumerableCalc(expr#0..2=[{inputs}], _id1=[$t0])\n"
//                        + "  EnumerableLimit(fetch=[1])\n"
//                        + "    EnumerableJoin(condition=[=($1, $2)], joinType=[inner])\n"
//                        + "      GeodeToEnumerableConverter\n"
//                        + "        GeodeProject(_id=[$0], _id0=[CAST($0):VARCHAR CHARACTER SET "
//                        + "\"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n"
//                        + "          GeodeScan(table=[[geode, zips]])\n"
//                        + "      GeodeToEnumerableConverter\n"
//                        + "        GeodeProject(_id0=[CAST($0):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
//                        + "\"ISO-8859-1$en_US$primary\"])\n"
//                        + "          GeodeScan(table=[[geode, zips]])\n" );
//    }
//
//
//    @Test
//    public void testSelectLocItem() {
//        polyphenyDbAssert()
//                .query( "SELECT loc[0] as lat, loc[1] as lon FROM view LIMIT 1" )
//                .returns( "lat=-105.007985; lon=39.840562\n" )
//                .explainContains( "PLAN=GeodeToEnumerableConverter\n"
//                        + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
//                        + "    GeodeSort(fetch=[1])\n"
//                        + "      GeodeScan(table=[[geode, zips]])\n" );
//    }
//
//
//    @Test
//    public void testItemPredicate() {
//        polyphenyDbAssert()
//                .query( "SELECT loc[0] as lat, loc[1] as lon FROM view WHERE loc[0] < 0 LIMIT 1" )
//                .returnsCount( 1 )
//                .returns( "lat=-105.007985; lon=39.840562\n" )
//                .explainContains( "PLAN=GeodeToEnumerableConverter\n"
//                        + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
//                        + "    GeodeSort(fetch=[1])\n"
//                        + "      GeodeFilter(condition=[<(ITEM($2, 0), 0)])\n"
//                        + "        GeodeScan(table=[[geode, zips]])\n" )
//                .queryContains( GeodeAssertions.query( "SELECT loc[0] AS lat, loc[1] AS lon FROM /zips WHERE loc[0] < 0 LIMIT 1" ) );
//
//        polyphenyDbAssert()
//                .query( "SELECT loc[0] as lat, loc[1] as lon FROM view WHERE loc[0] > 0 LIMIT 1" )
//                .returnsCount( 0 )
//                .explainContains( "PLAN=GeodeToEnumerableConverter\n"
//                        + "  GeodeProject(lat=[ITEM($2, 0)], lon=[ITEM($2, 1)])\n"
//                        + "    GeodeSort(fetch=[1])\n"
//                        + "      GeodeFilter(condition=[>(ITEM($2, 0), 0)])\n"
//                        + "        GeodeScan(table=[[geode, zips]])\n" )
//                .queryContains( GeodeAssertions.query( "SELECT loc[0] AS lat, loc[1] AS lon FROM /zips WHERE loc[0] > 0 LIMIT 1" ) );
//    }
//
//
//    @Test
//    public void testWhereWithOrForStringField() {
//        String expectedQuery = "SELECT state AS state FROM /zips WHERE state IN SET('MA', 'RI')";
//        polyphenyDbAssert()
//                .query( "SELECT state as state FROM view WHERE state = 'MA' OR state = 'RI'" )
//                .returnsCount( 6 )
//                .queryContains( GeodeAssertions.query( expectedQuery ) );
//    }
//
//
//    @Test
//    public void testWhereWithOrForNumericField() {
//        polyphenyDbAssert()
//                .query( "SELECT pop as pop FROM view WHERE pop = 34035 OR pop = 40173" )
//                .returnsCount( 2 )
//                .queryContains( GeodeAssertions.query( "SELECT pop AS pop FROM /zips WHERE pop IN SET(34035, 40173)" ) );
//    }
//
//
//    @Test
//    public void testWhereWithOrForNestedNumericField() {
//        String expectedQuery = "SELECT loc[1] AS lan FROM /zips WHERE loc[1] IN SET(43.218525, 44.098538)";
//
//        polyphenyDbAssert()
//                .query( "SELECT loc[1] as lan FROM view WHERE loc[1] = 43.218525 OR loc[1] = 44.098538" )
//                .returnsCount( 2 )
//                .queryContains( GeodeAssertions.query( expectedQuery ) );
//    }
//
//
//    @Test
//    public void testWhereWithOrForLargeValueList() throws Exception {
//        Cache cache = POLICY.cache();
//        QueryService queryService = cache.getQueryService();
//        Query query = queryService.newQuery( "select state as state from /zips" );
//        SelectResults results = (SelectResults) query.execute();
//
//        Set<String> stateList = (Set<String>) results.stream().map( s -> {
//            StructImpl struct = (StructImpl) s;
//            return struct.get( "state" );
//        } ).collect( Collectors.toCollection( LinkedHashSet::new ) );
//
//        String stateListPredicate = stateList.stream()
//                .map( s -> String.format( Locale.ROOT, "state = '%s'", s ) )
//                .collect( Collectors.joining( " OR " ) );
//
//        String stateListStr = "'" + String.join( "', '", stateList ) + "'";
//
//        String queryToBeExecuted = "SELECT state as state FROM view WHERE " + stateListPredicate;
//        String expectedQuery = "SELECT state AS state FROM /zips WHERE state IN SET(" + stateListStr + ")";
//
//        polyphenyDbAssert()
//                .query( queryToBeExecuted )
//                .returnsCount( 149 )
//                .queryContains( GeodeAssertions.query( expectedQuery ) );
//    }
//
//
//    @Test
//    public void testSqlSingleStringWhereFilter() {
//        String expectedQuery = "SELECT state AS state FROM /zips WHERE state = 'NY'";
//        polyphenyDbAssert()
//                .query( "SELECT state as state FROM view WHERE state = 'NY'" )
//                .returnsCount( 3 )
//                .queryContains( GeodeAssertions.query( expectedQuery ) );
//    }
//
//
//    @Test
//    public void testWhereWithOrWithEmptyResult() {
//        String expectedQuery = "SELECT state AS state FROM /zips WHERE state IN SET('', null, true, false, 123, 13.892)";
//        polyphenyDbAssert()
//                .query( "SELECT state as state FROM view WHERE state = '' OR state = null OR state = true OR state = false OR state = true OR state = 123 OR state = 13.892" )
//                .returnsCount( 0 )
//                .queryContains( GeodeAssertions.query( expectedQuery ) );
//    }
}

