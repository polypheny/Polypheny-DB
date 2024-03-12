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

package org.polypheny.db.sql.validate;


import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Tests for {@link SqlValidatorUtil}.
 */
public class SqlValidatorUtilTest {

    private static void checkChangedFieldList( List<String> nameList, List<String> resultList, boolean caseSensitive ) {
        // Check that the new names are appended with "0" in order they appear in original nameList. This is assuming that we only have one "collision".
        int i = 0;
        for ( String name : nameList ) {
            String newName = resultList.get( i );
            assertThat( newName, anyOf( is( name ), is( name + "0" ) ) );
            i++;
        }

        // Make sure each name is unique
        List<String> copyResultList = new ArrayList<>( resultList.size() );
        for ( String result : resultList ) {
            copyResultList.add( result.toLowerCase( Locale.ROOT ) );
        }

        for ( String result : resultList ) {
            final String lowerResult = result.toLowerCase( Locale.ROOT );
            assertThat( copyResultList.contains( lowerResult ), is( true ) );
            copyResultList.remove( lowerResult );
            if ( !caseSensitive ) {
                assertThat( copyResultList.contains( lowerResult ), is( false ) );
            }
        }
        assertThat( copyResultList.size(), is( 0 ) );
    }


    @Test
    public void testUniquifyCaseSensitive() {
        List<String> nameList = Lists.newArrayList( "col1", "COL1", "col_ABC", "col_abC" );
        List<String> resultList = ValidatorUtil.uniquify( nameList, ValidatorUtil.EXPR_SUGGESTER, true );
        assertThat( nameList, sameInstance( resultList ) );
    }


    @Test
    public void testUniquifyNotCaseSensitive() {
        List<String> nameList = Lists.newArrayList( "col1", "COL1", "col_ABC", "col_abC" );
        List<String> resultList = ValidatorUtil.uniquify( nameList, ValidatorUtil.EXPR_SUGGESTER, false );
        assertThat( resultList, not( nameList ) );
        checkChangedFieldList( nameList, resultList, false );
    }


    @Test
    public void testUniquifyOrderingCaseSensitive() {
        List<String> nameList = Lists.newArrayList( "k68s", "def", "col1", "COL1", "abc", "123" );
        List<String> resultList = ValidatorUtil.uniquify( nameList, ValidatorUtil.EXPR_SUGGESTER, true );
        assertThat( nameList, sameInstance( resultList ) );
    }


    @Test
    public void testUniquifyOrderingRepeatedCaseSensitive() {
        List<String> nameList = Lists.newArrayList( "k68s", "def", "col1", "COL1", "def", "123" );
        List<String> resultList = ValidatorUtil.uniquify( nameList, ValidatorUtil.EXPR_SUGGESTER, true );
        assertThat( nameList, not( resultList ) );
        checkChangedFieldList( nameList, resultList, true );
    }


    @Test
    public void testUniquifyOrderingNotCaseSensitive() {
        List<String> nameList = Lists.newArrayList( "k68s", "def", "col1", "COL1", "abc", "123" );
        List<String> resultList = ValidatorUtil.uniquify( nameList, ValidatorUtil.EXPR_SUGGESTER, false );
        assertThat( resultList, not( nameList ) );
        checkChangedFieldList( nameList, resultList, false );
    }


    @Test
    public void testUniquifyOrderingRepeatedNotCaseSensitive() {
        List<String> nameList = Lists.newArrayList( "k68s", "def", "col1", "COL1", "def", "123" );
        List<String> resultList = ValidatorUtil.uniquify( nameList, ValidatorUtil.EXPR_SUGGESTER, false );
        assertThat( resultList, not( nameList ) );
        checkChangedFieldList( nameList, resultList, false );
    }



    @Test
    public void testNameMatcher() {
        final ImmutableList<String> beatles = ImmutableList.of( "john", "paul", "ringo", "rinGo" );
        final NameMatcher insensitiveMatcher = NameMatchers.withCaseSensitive( false );
        assertThat( insensitiveMatcher.frequency( beatles, "ringo" ), is( 2 ) );
        assertThat( insensitiveMatcher.frequency( beatles, "rinGo" ), is( 2 ) );
        final NameMatcher sensitiveMatcher = NameMatchers.withCaseSensitive( true );
        assertThat( sensitiveMatcher.frequency( beatles, "ringo" ), is( 1 ) );
        assertThat( sensitiveMatcher.frequency( beatles, "rinGo" ), is( 1 ) );
        assertThat( sensitiveMatcher.frequency( beatles, "Ringo" ), is( 0 ) );
    }

}
