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

package org.polypheny.db.test;


import com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.TestUtil;
import org.polypheny.db.util.Util;


/**
 * Tests for the {@code org.polypheny.db.adapter.cassandra} package.
 *
 * Will start embedded cassandra cluster and populate it from local {@code twissandra.cql} file. All configuration files are located in test classpath.
 *
 * Note that tests will be skipped if running on JDK11 and JDK12 (which is not yet supported by cassandra) see
 * <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 */
@Ignore
public class CassandraAdapterTest {
    // TODO MV: enable
    /*
    @ClassRule
    public static final ExternalResource RULE = initCassandraIfEnabled();
*/
    /**
     * Connection factory based on the "mongo-zips" model.
     */
    private static final ImmutableMap<String, String> TWISSANDRA = ImmutableMap.of( "model", Sources.of( CassandraAdapterTest.class.getResource( "/model.json" ) ).file().getAbsolutePath() );


    /**
     * Whether to run this test.
     * Enabled by default, unless explicitly disabled from command line ({@code -Dpolyphenydb.test.cassandra=false}) or running on incompatible JDK version (see below).
     *
     * As of this wiring Cassandra 4.x is not yet released and we're using 3.x (which fails on JDK11 and JDK12). All cassandra tests will be skipped if running on JDK11 and JDK12.
     *
     * @return {@code true} if test is compatible with current environment, {@code false} otherwise
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>
     */
    private static boolean enabled() {
        final boolean enabled = Util.getBooleanProperty( "polyphenydb.test.cassandra", true );
        Bug.upgrade( "remove JDK version check once current adapter supports Cassandra 4.x" );
        final boolean compatibleJdk = TestUtil.getJavaMajorVersion() != 11 && TestUtil.getJavaMajorVersion() != 12;
        return enabled && compatibleJdk;
    }

    // TODO MV: enable
/*
    private static ExternalResource initCassandraIfEnabled() {
        if ( !enabled() ) {
            // Return NOP resource (to avoid nulls)
            return new ExternalResource() {
                @Override
                public Statement apply( final Statement base, final Description description ) {
                    return super.apply( base, description );
                }
            };
        }

        String configurationFileName = "cassandra.yaml"; // use default one
        // Apache Jenkins often fails with "CassandraAdapterTest Cassandra daemon did not start within timeout (20 sec by default)"
        long startUpTimeoutMillis = TimeUnit.SECONDS.toMillis( 60 );

        CassandraCQLUnit rule = new CassandraCQLUnit( new ClassPathCQLDataSet( "twissandra.cql" ), configurationFileName, startUpTimeoutMillis );

        // This static init is necessary otherwise tests fail with CassandraUnit in IntelliJ (jdk10) should be called right after constructor
        // NullPointerException for DatabaseDescriptor.getDiskFailurePolicy
        // for more info see
        // https://github.com/jsevellec/cassandra-unit/issues/249
        // https://github.com/jsevellec/cassandra-unit/issues/221
        DatabaseDescriptor.daemonInitialization();

        return rule;
    }


    @BeforeClass
    public static void setUp() {
        // run tests only if explicitly enabled
        assumeTrue( "test explicitly disabled", enabled() );
    }


    @Test
    public void testSelect() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select * from \"users\"" )
                .returnsCount( 10 );
    }


    @Test
    public void testFilter() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select * from \"userline\" where \"username\"='!PUBLIC!'" )
                .limit( 1 )
                .returns( "username=!PUBLIC!; time=e8754000-80b8-1fe9-8e73-e3698c967ddd; " + "tweet_id=f3c329de-d05b-11e5-b58b-90e2ba530b12\n" )
                .explainContains( "PLAN=CassandraToEnumerableConverter\n" + "  CassandraFilter(condition=[=($0, '!PUBLIC!')])\n" + "    CassandraScan(table=[[twissandra, userline]]" );
    }


    @Test
    public void testFilterUUID() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select * from \"tweets\" where \"tweet_id\"='f3cd759c-d05b-11e5-b58b-90e2ba530b12'" )
                .limit( 1 )
                .returns( "tweet_id=f3cd759c-d05b-11e5-b58b-90e2ba530b12; " + "body=Lacus augue pede posuere.; username=JmuhsAaMdw\n" )
                .explainContains( "PLAN=CassandraToEnumerableConverter\n" + "  CassandraFilter(condition=[=(CAST($0):CHAR(36), 'f3cd759c-d05b-11e5-b58b-90e2ba530b12')])\n" + "    CassandraScan(table=[[twissandra, tweets]]" );
    }


    @Test
    public void testSort() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select * from \"userline\" where \"username\" = '!PUBLIC!' order by \"time\" desc" )
                .returnsCount( 146 )
                .explainContains( "PLAN=CassandraToEnumerableConverter\n" + "  CassandraSort(sort0=[$1], dir0=[DESC])\n" + "    CassandraFilter(condition=[=($0, '!PUBLIC!')])\n" );
    }


    @Test
    public void testProject() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 2" )
                .returns( "tweet_id=f3c329de-d05b-11e5-b58b-90e2ba530b12\n" + "tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n" )
                .explainContains( "PLAN=CassandraToEnumerableConverter\n" + "  CassandraLimit(fetch=[2])\n" + "    CassandraProject(tweet_id=[$2])\n" + "      CassandraFilter(condition=[=($0, '!PUBLIC!')])\n" );
    }


    @Test
    public void testProjectAlias() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select \"tweet_id\" as \"foo\" from \"userline\" " + "where \"username\" = '!PUBLIC!' limit 1" )
                .returns( "foo=f3c329de-d05b-11e5-b58b-90e2ba530b12\n" );
    }


    @Test
    public void testProjectConstant() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select 'foo' as \"bar\" from \"userline\" limit 1" )
                .returns( "bar=foo\n" );
    }


    @Test
    public void testLimit() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select \"tweet_id\" from \"userline\" where \"username\" = '!PUBLIC!' limit 8" )
                .explainContains( "CassandraLimit(fetch=[8])\n" );
    }


    @Test
    public void testSortLimit() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select * from \"userline\" where \"username\"='!PUBLIC!' " + "order by \"time\" desc limit 10" )
                .explainContains( "  CassandraLimit(fetch=[10])\n" + "    CassandraSort(sort0=[$1], dir0=[DESC])" );
    }


    @Test
    public void testSortOffset() {
        PolyphenyDbAssert.that()
                .with( TWISSANDRA )
                .query( "select \"tweet_id\" from \"userline\" where " + "\"username\"='!PUBLIC!' limit 2 offset 1" )
                .explainContains( "CassandraLimit(offset=[1], fetch=[2])" )
                .returns( "tweet_id=f3dbb03a-d05b-11e5-b58b-90e2ba530b12\n" + "tweet_id=f3e4182e-d05b-11e5-b58b-90e2ba530b12\n" );
    }
*/
}