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


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.piglet.parser.ParseException;


/**
 * Unit tests for Piglet Parser.
 */
public class PigletParserTest {

    private static Fluent pig( String pig ) {
        return new Fluent( pig );
    }


    @Test
    public void testParseLoad() throws ParseException {
        final String s = "A = LOAD 'Emp';";
        final String expected = "{op: PROGRAM, stmts: [\n" + "  {op: LOAD, target: A, name: Emp}]}";
        pig( s ).parseContains( expected );
    }


    /**
     * Tests parsing and un-parsing all kinds of operators.
     */
    @Test
    public void testParse2() throws ParseException {
        final String s = """
                A = LOAD 'Emp';
                DESCRIBE A;
                DUMP A;
                B = FOREACH A GENERATE 1, name;
                B1 = FOREACH A {
                  X = DISTINCT A;
                  Y = FILTER X BY foo;
                  Z = LIMIT Z 3;
                  GENERATE 1, name;
                }
                C = FILTER B BY name;
                D = DISTINCT C;
                E = ORDER D BY $1 DESC, $2 ASC, $3;
                F = ORDER E BY * DESC;
                G = LIMIT F -10;
                H = GROUP G ALL;
                I = GROUP H BY e;
                J = GROUP I BY (e1, e2);
                """;
        final String expected = """
                {op: PROGRAM, stmts: [
                  {op: LOAD, target: A, name: Emp},
                  {op: DESCRIBE, relation: A},
                  {op: DUMP, relation: A},
                  {op: FOREACH, target: B, source: A, expList: [
                    1,
                    name]},
                  {op: FOREACH, target: B1, source: A, nestedOps: [
                    {op: DISTINCT, target: X, source: A},
                    {op: FILTER, target: Y, source: X, condition: foo},
                    {op: LIMIT, target: Z, source: Z, count: 3}], expList: [
                    1,
                    name]},
                  {op: FILTER, target: C, source: B, condition: name},
                  {op: DISTINCT, target: D, source: C},
                  {op: ORDER, target: E, source: D},
                  {op: ORDER, target: F, source: E},
                  {op: LIMIT, target: G, source: F, count: -10},
                  {op: GROUP, target: H, source: G},
                  {op: GROUP, target: I, source: H, keys: [
                    e]},
                  {op: GROUP, target: J, source: I, keys: [
                    e1,
                    e2]}]}""";
        pig( s ).parseContains( expected );
    }


    @Test
    public void testScan() throws ParseException {
        final String s = "A = LOAD 'EMP';";
        final String expected = "LogicalScan(table=[[scott, EMP]])\n";
        pig( s ).explainContains( expected );
    }


    @Test
    public void testDump() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "DUMP A;";
        final String expected = "LogicalScan(table=[[scott, DEPT]])\n";
        final String out = """
                (10,ACCOUNTING,NEW YORK)
                (20,RESEARCH,DALLAS)
                (30,SALES,CHICAGO)
                (40,OPERATIONS,BOSTON)
                """;
        pig( s ).explainContains( expected ).returns( out );
    }


    /**
     * VALUES is an extension to Pig. You can achieve the same effect in standard Pig by creating a text file.
     */
    @Test
    public void testDumpValues() throws ParseException {
        final String s = "A = VALUES (1, 'a'), (2, 'b') AS (x: int, y: string);\n" + "DUMP A;";
        final String expected = "LogicalValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";
        final String out = "(1,a)\n(2,b)\n";
        pig( s ).explainContains( expected ).returns( out );
    }


    @Test
    public void testForeach() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "B = FOREACH A GENERATE DNAME, $2;";
        final String expected = """
                LogicalProject(DNAME=[$1], LOC=[$2])
                  LogicalScan(table=[[scott, DEPT]])
                """;
        pig( s ).explainContains( expected );
    }


    @Disabled // foreach nested not implemented yet
    @Test
    public void testForeachNested() throws ParseException {
        final String s = """
                A = LOAD 'EMP';
                B = GROUP A BY DEPTNO;
                C = FOREACH B {
                  D = ORDER A BY SAL DESC;
                  E = LIMIT D 3;
                  GENERATE E.DEPTNO, E.EMPNO;
                }""";
        final String expected = """
                LogicalProject(DNAME=[$1], LOC=[$2])
                  LogicalScan(table=[[scott, DEPT]])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testGroup() throws ParseException {
        final String s = "A = LOAD 'EMP';\n" + "B = GROUP A BY DEPTNO;";
        final String expected = """
                LogicalAggregate(group=[{7}], A=[COLLECT($8)])
                  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])
                    LogicalScan(table=[[scott, EMP]])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testGroupExample() throws ParseException {
        final String pre = """
                A = VALUES ('John',18,4.0F),
                ('Mary',19,3.8F),
                ('Bill',20,3.9F),
                ('Joe',18,3.8F) AS (name:chararray,age:int,gpa:float);
                """;
        final String b = pre
                + "B = GROUP A BY age;\n"
                + "DUMP B;\n";
        pig( b ).returnsUnordered(
                "(18,{(John,18,4.0F),(Joe,18,3.8F)})",
                "(19,{(Mary,19,3.8F)})",
                "(20,{(Bill,20,3.9F)})" );
    }


    @Test
    public void testDistinctExample() throws ParseException {
        final String pre = """
                A = VALUES (8,3,4),
                (1,2,3),
                (4,3,3),
                (4,3,3),
                (1,2,3) AS (a1:int,a2:int,a3:int);
                """;
        final String x = pre
                + "X = DISTINCT A;\n"
                + "DUMP X;\n";
        pig( x ).returnsUnordered( "(1,2,3)", "(4,3,3)", "(8,3,4)" );
    }


    @Test
    public void testFilter() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "B = FILTER A BY DEPTNO;";
        final String expected = """
                LogicalFilter(condition=[$0])
                  LogicalScan(table=[[scott, DEPT]])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testFilterExample() throws ParseException {
        final String pre = """
                A = VALUES (1,2,3),
                (4,2,1),
                (8,3,4),
                (4,3,3),
                (7,2,5),
                (8,4,3) AS (f1:int,f2:int,f3:int);
                """;

        final String x = pre
                + "X = FILTER A BY f3 == 3;\n"
                + "DUMP X;\n";
        final String expected = """
                (1,2,3)
                (4,3,3)
                (8,4,3)
                """;
        pig( x ).returns( expected );

        final String x2 = pre
                + "X2 = FILTER A BY (f1 == 8) OR (NOT (f2+f3 > f1));\n"
                + "DUMP X2;\n";
        final String expected2 = """
                (4,2,1)
                (8,3,4)
                (7,2,5)
                (8,4,3)
                """;
        pig( x2 ).returns( expected2 );
    }


    @Test
    public void testLimit() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "B = LIMIT A 3;";
        final String expected = """
                LogicalSort(fetch=[3])
                  LogicalScan(table=[[scott, DEPT]])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testLimitExample() throws ParseException {
        final String pre = """
                A = VALUES (1,2,3),
                (4,2,1),
                (8,3,4),
                (4,3,3),
                (7,2,5),
                (8,4,3) AS (f1:int,f2:int,f3:int);
                """;

        final String x = pre
                + "X = LIMIT A 3;\n"
                + "DUMP X;\n";
        final String expected = """
                (1,2,3)
                (4,2,1)
                (8,3,4)
                """;
        pig( x ).returns( expected );

        final String x2 = pre
                + "B = ORDER A BY f1 DESC, f2 ASC;\n"
                + "X2 = LIMIT B 3;\n"
                + "DUMP X2;\n";
        final String expected2 = """
                (8,3,4)
                (8,4,3)
                (7,2,5)
                """;
        pig( x2 ).returns( expected2 );
    }


    @Test
    public void testOrder() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n"
                + "B = ORDER A BY DEPTNO DESC, DNAME;";
        final String expected = """
                LogicalSort(sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[ASC])
                  LogicalScan(table=[[scott, DEPT]])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testOrderStar() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n"
                + "B = ORDER A BY * DESC;";
        final String expected = """
                LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[DESC], dir1=[DESC], dir2=[DESC])
                  LogicalScan(table=[[scott, DEPT]])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testOrderExample() throws ParseException {
        final String pre = """
                A = VALUES (1,2,3),
                (4,2,1),
                (8,3,4),
                (4,3,3),
                (7,2,5),
                (8,4,3) AS (a1:int,a2:int,a3:int);
                """;

        final String x = pre
                + "X = ORDER A BY a3 DESC;\n"
                + "DUMP X;\n";
        final String expected = """
                (7,2,5)
                (8,3,4)
                (1,2,3)
                (4,3,3)
                (8,4,3)
                (4,2,1)
                """;
        pig( x ).returns( expected );
    }


    /**
     * VALUES is an extension to Pig. You can achieve the same effect in standard Pig by creating a text file.
     */
    @Test
    public void testValues() throws ParseException {
        final String s = "A = VALUES (1, 'a'), (2, 'b') AS (x: int, y: string);\n" + "DUMP A;";
        final String expected = "LogicalValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";
        pig( s ).explainContains( expected );
    }


    @Test
    public void testValuesNested() throws ParseException {
        final String s = """
                A = VALUES (1, {('a', true), ('b', false)}),
                 (2, {})
                AS (x: int, y: bag {tuple(a: string, b: boolean)});
                DUMP A;""";
        final String expected = "LogicalValues(tuples=[[{ 1, [['a', true], ['b', false]] }, { 2, [] }]])\n";
        pig( s ).explainContains( expected );
    }

}
