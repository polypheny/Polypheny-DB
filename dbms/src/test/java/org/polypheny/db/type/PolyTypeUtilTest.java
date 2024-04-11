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

package org.polypheny.db.type;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;


/**
 * Test of {@link PolyTypeUtil}.
 */
public class PolyTypeUtilTest {

    private final SqlTypeFixture f = new SqlTypeFixture();


    @Test
    public void testTypesIsSameFamilyWithNumberTypes() {
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlBigInt, f.sqlBigInt ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlInt, f.sqlBigInt ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlFloat, f.sqlBigInt ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlInt, f.sqlBigIntNullable ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithCharTypes() {
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlVarchar, f.sqlVarchar ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlVarchar, f.sqlChar ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlVarchar, f.sqlVarcharNullable ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithInconvertibleTypes() {
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlBoolean, f.sqlBigInt ) ), is( false ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlFloat, f.sqlBoolean ) ), is( false ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( f.sqlInt, f.sqlDate ) ), is( false ) );
    }


    @Test
    public void testTypesIsSameFamilyWithNumberStructTypes() {
        final AlgDataType bigIntAndFloat = struct( f.sqlBigInt, f.sqlFloat );
        final AlgDataType floatAndBigInt = struct( f.sqlFloat, f.sqlBigInt );

        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( bigIntAndFloat, floatAndBigInt ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( bigIntAndFloat, bigIntAndFloat ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( bigIntAndFloat, bigIntAndFloat ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( floatAndBigInt, floatAndBigInt ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithCharStructTypes() {
        final AlgDataType varCharStruct = struct( f.sqlVarchar );
        final AlgDataType charStruct = struct( f.sqlChar );

        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( varCharStruct, charStruct ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( charStruct, varCharStruct ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( varCharStruct, varCharStruct ) ), is( true ) );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( charStruct, charStruct ) ), is( true ) );
    }


    @Test
    public void testTypesIsSameFamilyWithInconvertibleStructTypes() {
        final AlgDataType dateStruct = struct( f.sqlDate );
        final AlgDataType boolStruct = struct( f.sqlBoolean );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( dateStruct, boolStruct ) ), is( false ) );

        final AlgDataType charIntStruct = struct( f.sqlChar, f.sqlInt );
        final AlgDataType charDateStruct = struct( f.sqlChar, f.sqlDate );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( charIntStruct, charDateStruct ) ), is( false ) );

        final AlgDataType boolDateStruct = struct( f.sqlBoolean, f.sqlDate );
        final AlgDataType floatIntStruct = struct( f.sqlInt, f.sqlFloat );
        assertThat( PolyTypeUtil.areSameFamily( ImmutableList.of( boolDateStruct, floatIntStruct ) ), is( false ) );
    }


    private AlgDataType struct( AlgDataType... algDataTypes ) {
        final Builder builder = f.typeFactory.builder();
        for ( int i = 0; i < algDataTypes.length; i++ ) {
            builder.add( null, "field" + i, null, algDataTypes[i] );
        }
        return builder.build();
    }

}

