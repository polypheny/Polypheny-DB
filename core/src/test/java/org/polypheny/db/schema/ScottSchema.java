/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.schema;


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

