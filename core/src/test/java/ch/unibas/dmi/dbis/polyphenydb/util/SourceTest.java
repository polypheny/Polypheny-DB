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


import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import org.junit.Test;


/**
 * Tests for {@link Source}.
 */
public class SourceTest {

    private static final String ROOT_PREFIX = getRootPrefix();


    private static String getRootPrefix() {
        for ( String s : new String[]{ "/", "c:/" } ) {
            if ( new File( s ).isAbsolute() ) {
                return s;
            }
        }
        throw new IllegalStateException( "Unsupported operation system detected. Both / and c:/ produce relative paths" );
    }


    @Test
    public void testAppendWithSpaces() {
        String fooRelative = "fo o+";
        String fooAbsolute = ROOT_PREFIX + "fo o+";
        String barRelative = "b ar+";
        String barAbsolute = ROOT_PREFIX + "b ar+";
        assertAppend( Sources.file( null, fooRelative ), Sources.file( null, barRelative ), "fo o+/b ar+" );
        assertAppend( Sources.file( null, fooRelative ), Sources.file( null, barAbsolute ), barAbsolute );
        assertAppend( Sources.file( null, fooAbsolute ), Sources.file( null, barRelative ), ROOT_PREFIX + "fo o+/b ar+" );
        assertAppend( Sources.file( null, fooAbsolute ), Sources.file( null, barAbsolute ), barAbsolute );

        String urlFooRelative = "file:fo%20o+";
        String urlFooAbsolute = "file:" + ROOT_PREFIX + "fo%20o+";
        String urlBarRelative = "file:b%20ar+";
        String urlBarAbsolute = "file:" + ROOT_PREFIX + "b%20ar+";
        assertAppend( Sources.url( urlFooRelative ), Sources.url( urlBarRelative ), "fo o+/b ar+" );
        assertAppend( Sources.url( urlFooRelative ), Sources.url( urlBarAbsolute ), barAbsolute );
        assertAppend( Sources.url( urlFooAbsolute ), Sources.url( urlBarRelative ), ROOT_PREFIX + "fo o+/b ar+" );
        assertAppend( Sources.url( urlFooAbsolute ), Sources.url( urlBarAbsolute ), barAbsolute );

        assertAppend( Sources.file( null, fooRelative ), Sources.url( urlBarRelative ), "fo o+/b ar+" );
        assertAppend( Sources.file( null, fooRelative ), Sources.url( urlBarAbsolute ), barAbsolute );
        assertAppend( Sources.file( null, fooAbsolute ), Sources.url( urlBarRelative ), ROOT_PREFIX + "fo o+/b ar+" );
        assertAppend( Sources.file( null, fooAbsolute ), Sources.url( urlBarAbsolute ), barAbsolute );

        assertAppend( Sources.url( urlFooRelative ), Sources.file( null, barRelative ), "fo o+/b ar+" );
        assertAppend( Sources.url( urlFooRelative ), Sources.file( null, barAbsolute ), barAbsolute );
        assertAppend( Sources.url( urlFooAbsolute ), Sources.file( null, barRelative ), ROOT_PREFIX + "fo o+/b ar+" );
        assertAppend( Sources.url( urlFooAbsolute ), Sources.file( null, barAbsolute ), barAbsolute );
    }


    @Test
    public void testAppendHttp() {
        // I've truly no idea what append of two URLs should be, yet it does something
        assertAppendUrl( Sources.url( "http://fo%20o+/ba%20r+" ), Sources.file( null, "no idea what I am doing+" ), "http://fo%20o+/ba%20r+/no%20idea%20what%20I%20am%20doing+" );
        assertAppendUrl( Sources.url( "http://fo%20o+" ), Sources.file( null, "no idea what I am doing+" ), "http://fo%20o+/no%20idea%20what%20I%20am%20doing+" );
        assertAppendUrl( Sources.url( "http://fo%20o+/ba%20r+" ), Sources.url( "file:no%20idea%20what%20I%20am%20doing+" ), "http://fo%20o+/ba%20r+/no%20idea%20what%20I%20am%20doing+" );
        assertAppendUrl( Sources.url( "http://fo%20o+" ), Sources.url( "file:no%20idea%20what%20I%20am%20doing+" ), "http://fo%20o+/no%20idea%20what%20I%20am%20doing+" );
    }


    private void assertAppend( Source parent, Source child, String expected ) {
        assertThat(
                parent + ".append(" + child + ")",
                parent.append( child ).file().toString(),
                // This should transparently support various OS
                is( new File( expected ).toString() ) );
    }


    private void assertAppendUrl( Source parent, Source child, String expected ) {
        assertThat(
                parent + ".append(" + child + ")",
                parent.append( child ).url().toString(),
                is( expected ) );
    }


    @Test
    public void testSpaceInUrl() {
        String url = "file:" + ROOT_PREFIX + "dir%20name/test%20file.json";
        final Source foo = Sources.url( url );
        assertEquals(
                url + " .file().getAbsolutePath()",
                new File( ROOT_PREFIX + "dir name/test file.json" ).getAbsolutePath(),
                foo.file().getAbsolutePath() );
    }


    @Test
    public void testSpaceInRelativeUrl() {
        String url = "file:dir%20name/test%20file.json";
        final Source foo = Sources.url( url );
        assertEquals(
                url + " .file().getAbsolutePath()",
                "dir name/test file.json",
                foo.file().getPath().replace( '\\', '/' ) );
    }


    @Test
    public void testRelative() {
        final Source fooBar = Sources.file( null, ROOT_PREFIX + "foo/bar" );
        final Source foo = Sources.file( null, ROOT_PREFIX + "foo" );
        final Source baz = Sources.file( null, ROOT_PREFIX + "baz" );
        final Source bar = fooBar.relative( foo );
        assertThat( bar.file().toString(), is( "bar" ) );
        assertThat( fooBar.relative( baz ), is( fooBar ) );
    }
}

