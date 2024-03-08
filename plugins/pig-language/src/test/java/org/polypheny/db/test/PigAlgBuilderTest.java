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

package org.polypheny.db.test;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.reflect.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.schemas.HrSnapshot;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.PigAlgBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.Util;


/**
 * Unit test for {@link PigAlgBuilder}.
 */

public class PigAlgBuilderTest {

    public static TestHelper helper;
    private static Transaction transaction;
    private static PigAlgBuilder builder;


    @BeforeAll
    public static void init() throws NoSuchFieldException {
        helper = TestHelper.getInstance();
        transaction = helper.getTransaction();
        builder = PigAlgBuilder.create( transaction.createStatement() );

        Field f = AlgBuilder.class.getDeclaredField( "snapshot" );
        f.setAccessible( true );
        try {
            f.set( builder, new HrSnapshot() );
        } catch ( IllegalAccessException e ) {
            throw new RuntimeException( e );
        }

    }


    @AfterAll
    public static void cleanUp() {
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Converts a relational expression to a sting with linux line-endings.
     */
    private String str( AlgNode r ) {
        return Util.toLinux( AlgOptUtil.toString( r ) );
    }


    @Test
    public void testScan() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp

        final AlgNode root = builder.scan( "emp" ).build();
        assertThat( str( root ), is( "LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])\n" ) );
    }



    @Test
    public void testDistinct() {
        // Syntax:
        //   alias = DISTINCT alias [PARTITION BY partitioner] [PARALLEL n];
        final AlgNode root = builder
                .scan( "emp" )
                .project( builder.field( "deptno" ) )
                .distinct()
                .build();
        final String plan = """
                LogicalRelAggregate(model=[RELATIONAL], group=[{0}])
                  LogicalRelProject(model=[RELATIONAL], deptno=[$1])
                    LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        assertThat( str( root ), is( plan ) );
    }


    @Test
    public void testFilter() {
        // Syntax:
        //  FILTER name BY expr
        // Example:
        //  output_var = FILTER input_var BY (field1 is not null);
        final AlgNode root = builder
                .load( "EMP.csv", null, null )
                .filter( builder.isNotNull( builder.field( "mgr" ) ) )
                .build();
        final String plan = """
                LogicalRelFilter(model=[RELATIONAL], condition=[IS NOT NULL($3)])
                  LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        assertThat( str( root ), is( plan ) );
    }




    @Test
    public void testGroup() {
        // Syntax:
        //   alias = GROUP alias { ALL | BY expression}
        //     [, alias ALL | BY expression ...] [USING 'collected' | 'merge']
        //     [PARTITION BY partitioner] [PARALLEL n];
        // Equivalent to Pig Latin:
        //   r = GROUP e BY (deptno, job);
        final AlgNode root = builder
                .scan( "emp" )
                .group( null, null, -1, builder.groupKey( "deptno", "job" ).alias( "e" ) )
                .build();
        final String plan = """
                LogicalRelAggregate(model=[RELATIONAL], group=[{1, 2}], =[COLLECT($4)])
                  LogicalRelProject(model=[RELATIONAL], empno=[$0], deptno=[$1], job=[$2], mgr=[$3], $f4=[ROW($0, $1, $2, $3)])
                    LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        assertThat( str( root ), is( plan ) );
    }


    @Test
    public void testGroup2() {
        // Equivalent to Pig Latin:
        //   r = GROUP e BY deptno, d BY deptno;
        final AlgNode root = builder
                .scan( "emp" )
                .scan( "dept" )
                .group( null, null, -1,
                        builder.groupKey( "deptno" ).alias( "e" ),
                        builder.groupKey( "deptno" ).alias( "d" ) )
                .build();
        final String plan = """
                LogicalRelJoin(model=[RELATIONAL], condition=[=($0, $2)], joinType=[inner])
                  LogicalRelAggregate(model=[RELATIONAL], group=[{0}], =[COLLECT($4)])
                    LogicalRelProject(model=[RELATIONAL], empno=[$0], deptno=[$1], job=[$2], mgr=[$3], $f4=[ROW($0, $1, $2, $3)])
                      LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                  LogicalRelAggregate(model=[RELATIONAL], group=[{0}], =[COLLECT($1)])
                    LogicalRelProject(model=[RELATIONAL], deptno=[$0], $f1=[ROW($0)])
                      LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        assertThat( str( root ), is( plan ) );
    }



    @Test
    public void testLoad() {
        // Syntax:
        //   LOAD 'data' [USING function] [AS schema];
        // Equivalent to Pig Latin:
        //   LOAD 'EMPS.csv'
        final AlgNode root = builder
                .load( "EMP.csv", null, null )
                .build();
        assertThat( str( root ), is( "LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])\n" ) );
    }



}
