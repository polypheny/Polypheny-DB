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
 */

package org.polypheny.db.schemas;

import java.util.List;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.snapshot.MockLogicalNamespace;
import org.polypheny.db.snapshot.MockSnapshot;
import org.polypheny.db.type.PolyType;

public class HrSnapshot extends MockSnapshot {

    /*
        Mocksnapshot with the following layout
        ---------------------
        Table: "emps",
            column: "empid", null, factory.createJavaType( int.class ) )
            column: "deptno", null, factory.createJavaType( int.class ) )
            column: "name", null, factory.createJavaType( String.class ) )
            column: "salary", null, factory.createJavaType( int.class ) )
            column:"commission", null, factory.createJavaType( Integer.class ) )

        Values:
            new Object[]{ 100, 10, "Bill", 10000, 1000 },
            new Object[]{ 110, 10, "Theodore", 11500, 250 },
            new Object[]{ 150, 10, "Sebastian", 7000, null },
            new Object[]{ 200, 20, "Eric", 8000, 500 } )

        ---------------------
        Table: "depts",
            column: "deptno", null, factory.createJavaType( int.class ) )
            column: "name", null, factory.createJavaType( String.class ) )

        Values:
            new Object[]{ 10, "Sales" },
            new Object[]{ 30, "Marketing" },
            new Object[]{ 40, "HR" } )
     */


    public HrSnapshot() {
        super();

        mock( new MockLogicalNamespace( "public", DataModel.RELATIONAL, true ) );

        mock( new MockTable( "emps", List.of( "empid" ), List.of(
                new MockColumnInfo( "empid", false, PolyType.INTEGER ),
                new MockColumnInfo( "deptno", false, PolyType.INTEGER ),
                new MockColumnInfo( "name", false, PolyType.VARCHAR, 255 ),
                new MockColumnInfo( "salary", false, PolyType.INTEGER ),
                new MockColumnInfo( "commission", false, PolyType.INTEGER ) ) )
        );

        mock( new MockTable( "depts", List.of( "depno" ), List.of(
                new MockColumnInfo( "deptno", false, PolyType.INTEGER ),
                new MockColumnInfo( "name", false, PolyType.VARCHAR, 255 )
        ) ) );

        mock( new MockTable( "emp", List.of(), List.of(
                new MockColumnInfo( "empno", false, PolyType.INTEGER ),
                new MockColumnInfo( "deptno", false, PolyType.INTEGER ),
                new MockColumnInfo( "job", false, PolyType.VARCHAR, 255 ),
                new MockColumnInfo( "mgr", true, PolyType.VARCHAR, 255 )
        ) ) );

        mock( new MockTable( "dept", List.of(), List.of(
                new MockColumnInfo( "deptno", false, PolyType.INTEGER ),
                new MockColumnInfo( "dname", false, PolyType.VARCHAR, 255 )
        ) ) );
    }


}
