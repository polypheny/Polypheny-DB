/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.core.ChainedOperatorTable;
import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlOperatorBinding;
import org.polypheny.db.languages.sql.util.ListSqlOperatorTable;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;


/**
 * Mock operator table for testing purposes. Contains the standard SQL operator table, plus a list of operators.
 */
public class MockSqlOperatorTable extends ChainedOperatorTable {

    private final ListSqlOperatorTable listOpTab;


    public MockSqlOperatorTable( OperatorTable parentTable ) {
        super( ImmutableList.of( parentTable, new ListSqlOperatorTable() ) );
        listOpTab = (ListSqlOperatorTable) tableList.get( 1 );
    }


    /**
     * Adds an operator to this table.
     */
    public void addOperator( SqlOperator op ) {
        listOpTab.add( op );
    }


    public static void addRamp( MockSqlOperatorTable opTab ) {
        // Don't use anonymous inner classes. They can't be instantiated using reflection when we are deserializing from JSON.
        opTab.addOperator( new RampFunction() );
        opTab.addOperator( new DedupFunction() );
    }


    /**
     * "RAMP" user-defined function.
     */
    public static class RampFunction extends SqlFunction {

        public RampFunction() {
            super( "RAMP", Kind.OTHER_FUNCTION, null, null, OperandTypes.NUMERIC, FunctionCategory.USER_DEFINED_FUNCTION );
        }


        @Override
        public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.builder()
                    .add( "I", null, PolyType.INTEGER )
                    .build();
        }

    }


    /**
     * "DEDUP" user-defined function.
     */
    public static class DedupFunction extends SqlFunction {

        public DedupFunction() {
            super( "DEDUP", Kind.OTHER_FUNCTION, null, null, OperandTypes.VARIADIC, FunctionCategory.USER_DEFINED_FUNCTION );
        }


        @Override
        public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.builder()
                    .add( "NAME", null, PolyType.VARCHAR, 1024 )
                    .build();
        }

    }

}

