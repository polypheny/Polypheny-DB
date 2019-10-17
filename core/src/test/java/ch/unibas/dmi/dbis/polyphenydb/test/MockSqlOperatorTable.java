/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.ChainedSqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.ListSqlOperatorTable;
import com.google.common.collect.ImmutableList;


/**
 * Mock operator table for testing purposes. Contains the standard SQL operator table, plus a list of operators.
 */
public class MockSqlOperatorTable extends ChainedSqlOperatorTable {

    private final ListSqlOperatorTable listOpTab;


    public MockSqlOperatorTable( SqlOperatorTable parentTable ) {
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
            super( "RAMP", SqlKind.OTHER_FUNCTION, null, null, OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION );
        }


        @Override
        public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.builder()
                    .add( "I", SqlTypeName.INTEGER )
                    .build();
        }
    }


    /**
     * "DEDUP" user-defined function.
     */
    public static class DedupFunction extends SqlFunction {

        public DedupFunction() {
            super( "DEDUP", SqlKind.OTHER_FUNCTION, null, null, OperandTypes.VARIADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION );
        }


        @Override
        public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            return typeFactory.builder()
                    .add( "NAME", SqlTypeName.VARCHAR, 1024 )
                    .build();
        }
    }
}

