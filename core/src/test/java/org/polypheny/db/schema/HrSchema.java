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


    public record Emps( int empid, int deptno, String name, int salary, int commission ) {

    }


    public record Depts( int deptno, String name, int employees, Location location ) {

    }


    public record Location( int x, int y ) {

    }


    public record Dependents( int empid, String name ) {

    }


}

