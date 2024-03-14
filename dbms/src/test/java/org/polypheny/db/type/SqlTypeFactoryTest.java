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


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.type.AlgDataType;


/**
 * Test for {@link PolyTypeFactoryImpl}.
 */
public class SqlTypeFactoryTest {

    @Test
    public void testLeastRestrictiveWithAny() {
        SqlTypeFixture f = new SqlTypeFixture();
        AlgDataType leastRestrictive = f.typeFactory.leastRestrictive( Lists.newArrayList( f.sqlBigInt, f.sqlAny ) );
        assertThat( leastRestrictive.getPolyType(), is( PolyType.ANY ) );
    }


    @Test
    public void testLeastRestrictiveWithNumbers() {
        SqlTypeFixture f = new SqlTypeFixture();
        AlgDataType leastRestrictive = f.typeFactory.leastRestrictive( Lists.newArrayList( f.sqlBigInt, f.sqlInt ) );
        assertThat( leastRestrictive.getPolyType(), is( PolyType.BIGINT ) );
    }


    @Test
    public void testLeastRestrictiveWithNullability() {
        SqlTypeFixture f = new SqlTypeFixture();
        AlgDataType leastRestrictive = f.typeFactory.leastRestrictive( Lists.newArrayList( f.sqlVarcharNullable, f.sqlAny ) );
        assertThat( leastRestrictive.getPolyType(), is( PolyType.ANY ) );
        assertThat( leastRestrictive.isNullable(), is( true ) );
    }


    @Test
    public void testLeastRestrictiveWithNull() {
        SqlTypeFixture f = new SqlTypeFixture();
        AlgDataType leastRestrictive = f.typeFactory.leastRestrictive( Lists.newArrayList( f.sqlNull, f.sqlNull ) );
        assertThat( leastRestrictive.getPolyType(), is( PolyType.NULL ) );
        assertThat( leastRestrictive.isNullable(), is( true ) );
    }


    /**
     * Unit test for {@link PolyTypeUtil#comparePrecision(int, int)} and  {@link PolyTypeUtil#maxPrecision(int, int)}.
     */
    @Test
    public void testMaxPrecision() {
        final int un = AlgDataType.PRECISION_NOT_SPECIFIED;
        checkPrecision( 1, 1, 1, 0 );
        checkPrecision( 2, 1, 2, 1 );
        checkPrecision( 2, 100, 100, -1 );
        checkPrecision( 2, un, un, -1 );
        checkPrecision( un, 2, un, 1 );
        checkPrecision( un, un, un, 0 );
    }


    /**
     * Unit test for {@link ArrayType#getPrecedenceList()}.
     */
    @Test
    public void testArrayPrecedenceList() {
        SqlTypeFixture f = new SqlTypeFixture();
        assertThat( checkPrecendenceList( f.arrayBigInt, f.arrayBigInt, f.arrayFloat ), is( 3 ) );
        assertThat( checkPrecendenceList( f.arrayOfArrayBigInt, f.arrayOfArrayBigInt, f.arrayOfArrayFloat ), is( 3 ) );
        assertThat( checkPrecendenceList( f.sqlBigInt, f.sqlBigInt, f.sqlFloat ), is( 3 ) );
        assertThat( checkPrecendenceList( f.multisetBigInt, f.multisetBigInt, f.multisetFloat ), is( 3 ) );
        assertThat( checkPrecendenceList( f.arrayBigInt, f.arrayBigInt, f.arrayBigIntNullable ), is( 0 ) );
        try {
            int i = checkPrecendenceList( f.arrayBigInt, f.sqlBigInt, f.sqlInt );
            fail( "Expected assert, got " + i );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "must contain type: BIGINT" ) );
        }
    }


    private int checkPrecendenceList( AlgDataType t, AlgDataType type1, AlgDataType type2 ) {
        return t.getPrecedenceList().compareTypePrecedence( type1, type2 );
    }


    private void checkPrecision( int p0, int p1, int expectedMax, int expectedComparison ) {
        assertThat( PolyTypeUtil.maxPrecision( p0, p1 ), is( expectedMax ) );
        assertThat( PolyTypeUtil.maxPrecision( p1, p0 ), is( expectedMax ) );
        assertThat( PolyTypeUtil.maxPrecision( p0, p0 ), is( p0 ) );
        assertThat( PolyTypeUtil.maxPrecision( p1, p1 ), is( p1 ) );
        assertThat( PolyTypeUtil.comparePrecision( p0, p1 ), is( expectedComparison ) );
        assertThat( PolyTypeUtil.comparePrecision( p0, p0 ), is( 0 ) );
        assertThat( PolyTypeUtil.comparePrecision( p1, p1 ), is( 0 ) );
    }

}

