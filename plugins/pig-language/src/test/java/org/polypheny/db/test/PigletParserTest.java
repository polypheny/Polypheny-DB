/*
 * Copyright 2019-2024 The Polypheny Project
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
public class PigletParserTest extends PigTestTemplate {

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
        final String expected = "LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])\n";
        pig( s ).explainContains( expected );
    }


    @Test
    public void testDump() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "DUMP A;";
        final String expected = "LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])\n";

        pig( s ).explainContains( expected );
    }


    /**
     * VALUES is an extension to Pig. You can achieve the same effect in standard Pig by creating a text file.
     */
    @Test
    public void testDumpValues() throws ParseException {
        final String s = "A = VALUES (1, 'a'), (2, 'b') AS (x: int, y: string);\n" + "DUMP A;";
        final String expected = "LogicalRelValues(model=[RELATIONAL], tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";
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
        final String s = "A = LOAD 'emp';\n" + "B = GROUP A BY deptno;";
        final String expected = """
                LogicalRelAggregate(model=[RELATIONAL], group=[{1}], A=[COLLECT($4)])
                  LogicalRelProject(model=[RELATIONAL], empno=[$0], deptno=[$1], job=[$2], mgr=[$3], $f4=[ROW($0, $1, $2, $3)])
                    LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testFilter() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "B = FILTER A BY deptno;";
        final String expected = """
                LogicalRelFilter(model=[RELATIONAL], condition=[$0])
                  LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testLimit() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n" + "B = LIMIT A 3;";
        final String expected = """
                LogicalRelSort(model=[RELATIONAL], fetch=[3])
                  LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testOrder() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n"
                + "B = ORDER A BY deptno DESC, dname;";
        final String expected = """
                LogicalRelSort(model=[RELATIONAL], sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[ASC])
                  LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        pig( s ).explainContains( expected );
    }


    @Test
    public void testOrderStar() throws ParseException {
        final String s = "A = LOAD 'DEPT';\n"
                + "B = ORDER A BY * DESC;";
        final String expected = """
                LogicalRelSort(model=[RELATIONAL], sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[DESC])
                  LogicalRelScan(model=[RELATIONAL], table=[0], layer=[LOGICAL])
                """;
        pig( s ).explainContains( expected );
    }


    /**
     * VALUES is an extension to Pig. You can achieve the same effect in standard Pig by creating a text file.
     */
    @Test
    public void testValues() throws ParseException {
        final String s = "A = VALUES (1, 'a'), (2, 'b') AS (x: int, y: string);\n" + "DUMP A;";
        final String expected = "LogicalRelValues(model=[RELATIONAL], tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";
        pig( s ).explainContains( expected );
    }


}
