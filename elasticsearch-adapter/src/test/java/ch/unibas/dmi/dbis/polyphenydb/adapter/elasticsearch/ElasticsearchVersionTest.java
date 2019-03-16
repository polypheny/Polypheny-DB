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
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Basic tests for parsing ES version in different formats
 */
public class ElasticsearchVersionTest {

    @Test
    public void versions() {
        assertEquals( ElasticsearchVersion.fromString( "2.3.4" ), ElasticsearchVersion.ES2 );
        assertEquals( ElasticsearchVersion.fromString( "2.0.0" ), ElasticsearchVersion.ES2 );
        assertEquals( ElasticsearchVersion.fromString( "5.6.1" ), ElasticsearchVersion.ES5 );
        assertEquals( ElasticsearchVersion.fromString( "6.0.1" ), ElasticsearchVersion.ES6 );
        assertEquals( ElasticsearchVersion.fromString( "7.0.1" ), ElasticsearchVersion.ES7 );
        assertEquals( ElasticsearchVersion.fromString( "111.0.1" ), ElasticsearchVersion.UNKNOWN );
        assertEquals( ElasticsearchVersion.fromString( "2020.12.12" ), ElasticsearchVersion.UNKNOWN );

        assertFails( "" );
        assertFails( "." );
        assertFails( ".1.2" );
        assertFails( "1.2" );
        assertFails( "0" );
        assertFails( "b" );
        assertFails( "a.b" );
        assertFails( "aa" );
        assertFails( "a.b.c" );
        assertFails( "2.2" );
        assertFails( "a.2" );
        assertFails( "2.2.0a" );
        assertFails( "2a.2.0" );
    }


    private static void assertFails( String version ) {
        try {
            ElasticsearchVersion.fromString( version );
            fail( String.format( Locale.ROOT, "Should fail for version %s", version ) );
        } catch ( IllegalArgumentException ignore ) {
            // expected
        }
    }
}

