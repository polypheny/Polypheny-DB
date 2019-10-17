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

package ch.unibas.dmi.dbis.polyphenydb.util;


import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;


/**
 * Unit test for {@link Permutation}.
 */
public class PermutationTestCase {

    public PermutationTestCase() {
    }


    @Test
    public void testOne() {
        final Permutation perm = new Permutation( 4 );
        assertEquals( "[0, 1, 2, 3]", perm.toString() );
        assertEquals( 4, perm.size() );

        perm.set( 0, 2 );
        assertEquals( "[2, 1, 0, 3]", perm.toString() );

        perm.set( 1, 0 );
        assertEquals( "[2, 0, 1, 3]", perm.toString() );

        final Permutation invPerm = perm.inverse();
        assertEquals( "[1, 2, 0, 3]", invPerm.toString() );

        // changing perm doesn't change inverse
        perm.set( 0, 0 );
        assertEquals( "[0, 2, 1, 3]", perm.toString() );
        assertEquals( "[1, 2, 0, 3]", invPerm.toString() );
    }


    @Test
    public void testTwo() {
        final Permutation perm = new Permutation( new int[]{ 3, 2, 0, 1 } );
        assertFalse( perm.isIdentity() );
        assertEquals( "[3, 2, 0, 1]", perm.toString() );

        Permutation perm2 = (Permutation) perm.clone();
        assertEquals( "[3, 2, 0, 1]", perm2.toString() );
        assertEquals( perm, perm2 );
        assertEquals( perm2, perm );

        perm.set( 2, 1 );
        assertEquals( "[3, 2, 1, 0]", perm.toString() );
        assertNotEquals( perm, perm2 );

        // clone not affected
        assertEquals( "[3, 2, 0, 1]", perm2.toString() );

        perm2.set( 2, 3 );
        assertEquals( "[0, 2, 3, 1]", perm2.toString() );
    }


    @Test
    public void testInsert() {
        Permutation perm = new Permutation( new int[]{ 3, 0, 4, 2, 1 } );
        perm.insertTarget( 2 );
        assertEquals( "[4, 0, 5, 3, 1, 2]", perm.toString() );

        // insert at start
        perm = new Permutation( new int[]{ 3, 0, 4, 2, 1 } );
        perm.insertTarget( 0 );
        assertEquals( "[4, 1, 5, 3, 2, 0]", perm.toString() );

        // insert at end
        perm = new Permutation( new int[]{ 3, 0, 4, 2, 1 } );
        perm.insertTarget( 5 );
        assertEquals( "[3, 0, 4, 2, 1, 5]", perm.toString() );

        // insert into empty
        perm = new Permutation( new int[]{} );
        perm.insertTarget( 0 );
        assertEquals( "[0]", perm.toString() );
    }


    @Test
    public void testEmpty() {
        final Permutation perm = new Permutation( 0 );
        assertTrue( perm.isIdentity() );
        assertEquals( "[]", perm.toString() );
        assertEquals( perm, perm );
        assertEquals( perm, perm.inverse() );

        try {
            perm.set( 1, 0 );
            fail( "expected exception" );
        } catch ( ArrayIndexOutOfBoundsException e ) {
            // success
        }

        try {
            perm.set( -1, 2 );
            fail( "expected exception" );
        } catch ( ArrayIndexOutOfBoundsException e ) {
            // success
        }
    }


    @Test
    public void testProjectPermutation() {
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final RexBuilder builder = new RexBuilder( typeFactory );
        final RelDataType doubleType = typeFactory.createSqlType( SqlTypeName.DOUBLE );

        // A project with [1, 1] is not a permutation, so should return null
        final Permutation perm = Project.getPermutation( 2, ImmutableList.of( builder.makeInputRef( doubleType, 1 ), builder.makeInputRef( doubleType, 1 ) ) );
        assertThat( perm, nullValue() );

        // A project with [0, 1, 0] is not a permutation, so should return null
        final Permutation perm1 = Project.getPermutation( 2, ImmutableList.of( builder.makeInputRef( doubleType, 0 ), builder.makeInputRef( doubleType, 1 ), builder.makeInputRef( doubleType, 0 ) ) );
        assertThat( perm1, nullValue() );

        // A project of [1, 0] is a valid permutation!
        final Permutation perm2 = Project.getPermutation( 2, ImmutableList.of( builder.makeInputRef( doubleType, 1 ), builder.makeInputRef( doubleType, 0 ) ) );
        assertThat( perm2, is( new Permutation( new int[]{ 1, 0 } ) ) );
    }
}
