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

package org.polypheny.db.util;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import org.junit.jupiter.api.Test;


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
                new File( ROOT_PREFIX + "dir name/test file.json" ).getAbsolutePath(),
                foo.file().getAbsolutePath(),
                url + " .file().getAbsolutePath()" );
    }


    @Test
    public void testSpaceInRelativeUrl() {
        String url = "file:dir%20name/test%20file.json";
        final Source foo = Sources.url( url );
        assertEquals(
                "dir name/test file.json",
                foo.file().getPath().replace( '\\', '/' ),
                url + " .file().getAbsolutePath()" );
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

