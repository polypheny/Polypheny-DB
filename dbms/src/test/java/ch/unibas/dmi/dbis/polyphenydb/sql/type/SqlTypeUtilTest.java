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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory.Builder;
import com.google.common.collect.ImmutableList;
import org.junit.Test;


/**
 * Test of {@link SqlTypeUtil}.
 */
public class SqlTypeUtilTest {

    private final SqlTypeFixture f = new SqlTypeFixture();


    @Test
    public void testTypesIsSameFamilyWithNumberTypes() {
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlBigInt, f.sqlBigInt ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlInt, f.sqlBigInt ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlFloat, f.sqlBigInt ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlInt, f.sqlBigIntNullable ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithCharTypes() {
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlVarchar, f.sqlVarchar ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlVarchar, f.sqlChar ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlVarchar, f.sqlVarcharNullable ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithInconvertibleTypes() {
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlBoolean, f.sqlBigInt ) ), is( false ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlFloat, f.sqlBoolean ) ), is( false ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( f.sqlInt, f.sqlDate ) ), is( false ) );
    }


    @Test
    public void testTypesIsSameFamilyWithNumberStructTypes() {
        final RelDataType bigIntAndFloat = struct( f.sqlBigInt, f.sqlFloat );
        final RelDataType floatAndBigInt = struct( f.sqlFloat, f.sqlBigInt );

        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( bigIntAndFloat, floatAndBigInt ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( bigIntAndFloat, bigIntAndFloat ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( bigIntAndFloat, bigIntAndFloat ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( floatAndBigInt, floatAndBigInt ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithCharStructTypes() {
        final RelDataType varCharStruct = struct( f.sqlVarchar );
        final RelDataType charStruct = struct( f.sqlChar );

        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( varCharStruct, charStruct ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( charStruct, varCharStruct ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( varCharStruct, varCharStruct ) ), is( true ) );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( charStruct, charStruct ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithInconvertibleStructTypes() {
        final RelDataType dateStruct = struct( f.sqlDate );
        final RelDataType boolStruct = struct( f.sqlBoolean );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( dateStruct, boolStruct ) ), is( false ) );

        final RelDataType charIntStruct = struct( f.sqlChar, f.sqlInt );
        final RelDataType charDateStruct = struct( f.sqlChar, f.sqlDate );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( charIntStruct, charDateStruct ) ), is( false ) );

        final RelDataType boolDateStruct = struct( f.sqlBoolean, f.sqlDate );
        final RelDataType floatIntStruct = struct( f.sqlInt, f.sqlFloat );
        assertThat( SqlTypeUtil.areSameFamily( ImmutableList.of( boolDateStruct, floatIntStruct ) ), is( false ) );
    }


    private RelDataType struct( RelDataType... relDataTypes ) {
        final Builder builder = f.typeFactory.builder();
        for ( int i = 0; i < relDataTypes.length; i++ ) {
            builder.add( "field" + i, relDataTypes[i] );
        }
        return builder.build();
    }
}

