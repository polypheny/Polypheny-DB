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


public class HrSchema {

    public final Emps[] emps = {
            new Emps( 100, 1, "Bill", 4000, 2 ),
            new Emps( 200, 2, "Eric", 2500, 3 ),
            new Emps( 150, 1, "Sebastian", 6000, 2 ),
            new Emps( 150, 4, "Hans", 4400, 10 ),
    };


    public final Depts[] depts = {
            new Depts( 1, "Sales", 2, new Location( 23, 0 ) ),
            new Depts( 2, "Marketing", 1, new Location( 767, 899 ) ),
            new Depts( 3, "Engineering", 0, new Location( 98, 99 ) ),
            new Depts( 4, "Empty", 1, new Location( 22, 33 ) ),
    };


    public final Dependents[] dependents = {
            new Dependents( 1, "Foo" ),
    };


    @AllArgsConstructor
    public static class Emps {

        public final int empid;
        public final int deptno;
        public final String name;
        public final int salary;
        public final int commission;
    }


    @AllArgsConstructor
    public static class Depts {

        public final int deptno;
        public final String name;
        public final int employees;
        public final Location location;
    }


    @AllArgsConstructor
    public static class Location {

        public final int x;
        public final int y;
    }


    @AllArgsConstructor
    public static class Dependents {

        public final int empid;
        public final String name;
    }


}

