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

package ch.unibas.dmi.dbis.polyphenydb.schema;


import lombok.AllArgsConstructor;


public class ScottSchema {

    public final Emp[] emp = {
            new Emp( 7369, "SMITH", "CLERK", 7902, "17-12-1980", 800, null, 20 ),
            new Emp( 7499, "ALLEN", "SALESMAN", 7698, "20-2-1981", 1600, (float) 300, 30 ),
            new Emp( 7521, "WARD", "SALESMAN", 7698, "22-2-1981", 1250, (float) 500, 30 ),
            new Emp( 7566, "JONES", "MANAGER", 7839, "2-4-1981", 2975, null, 20 ),
            new Emp( 7654, "MARTIN", "SALESMAN", 7698, "28-9-1981", 1250, (float) 1400, 30 ),
            new Emp( 7698, "BLAKE", "MANAGER", 7839, "1-5-1981", 2850, null, 30 ),
            new Emp( 7782, "CLARK", "MANAGER", 7839, "9-6-1981", 2450, null, 10 ),
            new Emp( 7788, "SCOTT", "ANALYST", 7566, "13-7-1987", 3000, null, 20 ),
            new Emp( 7839, "KING", "PRESIDENT", null, "17-11-1981", 5000, null, 10 ),
            new Emp( 7844, "TURNER", "SALESMAN", 7698, "8-9-1981", 1500, (float) 0, 30 ),
            new Emp( 7876, "ADAMS", "CLERK", 7788, "13-JUL-87", 1100, null, 20 ),
            new Emp( 7900, "JAMES", "CLERK", 7698, "3-12-1981", 950, null, 30 ),
            new Emp( 7902, "FORD", "ANALYST", 7566, "3-12-1981", 3000, null, 20 ),
            new Emp( 7934, "MILLER", "CLERK", 7782, "23-1-1982", 1300, null, 10 ),
    };


    public final Dept[] dept = {
            new Dept( 10, "ACCOUNTING", "NEW YORK" ),
            new Dept( 20, "RESEARCH", "DALLAS" ),
            new Dept( 30, "SALES", "CHICAGO" ),
            new Dept( 40, "OPERATIONS", "BOSTON" ),
    };


    @AllArgsConstructor
    public static class Emp {

        public final int empno;
        public final String ename;
        public final String job;
        public final Integer mgr;
        public final String hiredate;
        public final float sal;
        public final Float comm;
        public final int deptno;
    }


    @AllArgsConstructor
    public static class Dept {

        public final int deptno;
        public final String dname;
        public final String loc;
    }


}

