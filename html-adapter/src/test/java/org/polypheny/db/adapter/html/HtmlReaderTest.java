/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.html;


import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;
import org.jsoup.select.Elements;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.TestUtil;


/**
 * Unit tests for HtmlReader.
 */
public class HtmlReaderTest {

    private static final Source CITIES_SOURCE = Sources.url( "http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population" );

    private static final Source STATES_SOURCE = Sources.url( "http://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States" );


    /**
     * Converts a path that is relative to the module into a path that is relative to where the test is running.
     */
    public static String file( String s ) {
        if ( new File( "file" ).exists() ) {
            return "file/" + s;
        } else {
            return s;
        }
    }


    private static String resourcePath( String path ) throws Exception {
        return Sources.of( HtmlReaderTest.class.getResource( "/" + path ) ).file().getAbsolutePath();
    }


    /**
     * Tests {@link HtmlReader} URL instantiation - no path.
     */
    @Test
    public void testHtmlReaderUrlNoPath() throws HtmlReaderException {
        Assume.assumeTrue( HtmlSuite.hazNetwork() );

        // Under OpenJDK, test fails with the following, so skip test:
        //   javax.net.ssl.SSLHandshakeException:
        //   sun.security.validator.ValidatorException: PKIX path building failed:
        //   sun.security.provider.certpath.SunCertPathBuilderException:
        //   unable to find valid certification path to requested target
        final String r = System.getProperty( "java.runtime.name" );
        // http://openjdk.java.net/jeps/319 => root certificates are bundled with JEP 10
        Assume.assumeTrue( "Java 10+ should have root certificates (JEP 319). Runtime is " + r + ", Jave major version is " + TestUtil.getJavaMajorVersion(),
                !r.equals( "OpenJDK Runtime Environment" ) || TestUtil.getJavaMajorVersion() > 10 );

        HtmlReader t = new HtmlReader( STATES_SOURCE );
        t.refresh();
    }


    /**
     * Tests {@link HtmlReader} URL instantiation - with path.
     */
    @Ignore("Wikipedia format change breaks html adapter test")
    @Test
    public void testHtmlReaderUrlWithPath() throws HtmlReaderException {
        Assume.assumeTrue( HtmlSuite.hazNetwork() );
        HtmlReader t = new HtmlReader( CITIES_SOURCE, "#mw-content-text > table.wikitable.sortable", 0 );
        t.refresh();
    }


    /**
     * Tests {@link HtmlReader} URL fetch.
     */
    @Ignore("Wikipedia format change breaks html adapter test")
    @Test
    public void testHtmlReaderUrlFetch() throws HtmlReaderException {
        Assume.assumeTrue( HtmlSuite.hazNetwork() );
        HtmlReader t = new HtmlReader( STATES_SOURCE, "#mw-content-text > table.wikitable.sortable", 0 );
        int i = 0;
        for ( Elements row : t ) {
            i++;
        }
        assertThat( i, is( 51 ) );
    }


    /**
     * Tests failed {@link HtmlReader} instantiation - malformed URL.
     */
    @Test
    public void testHtmlReaderMalUrl() throws HtmlReaderException {
        try {
            final Source badSource = Sources.url( "bad" + CITIES_SOURCE.url() );
            fail( "expected exception, got " + badSource );
        } catch ( RuntimeException e ) {
            assertThat( e.getCause(), instanceOf( MalformedURLException.class ) );
            assertThat( e.getCause().getMessage(), is( "unknown protocol: badhttp" ) );
        }
    }


    /**
     * Tests failed {@link HtmlReader} instantiation - bad URL.
     */
    @Test(expected = HtmlReaderException.class)
    public void testHtmlReaderBadUrl() throws HtmlReaderException {
        final String uri = "http://ex.wikipedia.org/wiki/List_of_United_States_cities_by_population";
        HtmlReader t = new HtmlReader( Sources.url( uri ), "table:eq(4)" );
        t.refresh();
    }


    /**
     * Tests failed {@link HtmlReader} instantiation - bad selector.
     */
    @Test(expected = HtmlReaderException.class)
    public void testHtmlReaderBadSelector() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableOK.html" ) );
        HtmlReader t = new HtmlReader( source, "table:eq(1)" );
        t.refresh();
    }


    /**
     * Test {@link HtmlReader} with static file - headings.
     */
    @Test
    public void testHtmlReaderHeadings() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableOK.html" ) );
        HtmlReader t = new HtmlReader( source );
        Elements headings = t.getHeadings();
        assertTrue( headings.get( 1 ).text().equals( "H1" ) );
    }


    /**
     * Test {@link HtmlReader} with static file - data.
     */
    @Test
    public void testHtmlReaderData() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableOK.html" ) );
        HtmlReader t = new HtmlReader( source );
        Iterator<Elements> i = t.iterator();
        Elements row = i.next();
        assertTrue( row.get( 2 ).text().equals( "R0C2" ) );
        row = i.next();
        assertTrue( row.get( 0 ).text().equals( "R1C0" ) );
    }


    /**
     * Tests {@link HtmlReader} with bad static file - headings.
     */
    @Test
    public void testHtmlReaderHeadingsBadFile() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableNoTheadTbody.html" ) );
        HtmlReader t = new HtmlReader( source );
        Elements headings = t.getHeadings();
        assertTrue( headings.get( 1 ).text().equals( "H1" ) );
    }


    /**
     * Tests {@link HtmlReader} with bad static file - data.
     */
    @Test
    public void testHtmlReaderDataBadFile() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableNoTheadTbody.html" ) );
        HtmlReader t = new HtmlReader( source );
        Iterator<Elements> i = t.iterator();
        Elements row = i.next();
        assertTrue( row.get( 2 ).text().equals( "R0C2" ) );
        row = i.next();
        assertTrue( row.get( 0 ).text().equals( "R1C0" ) );
    }


    /**
     * Tests {@link HtmlReader} with no headings static file - data.
     */
    @Test
    public void testHtmlReaderDataNoTh() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableNoTH.html" ) );
        HtmlReader t = new HtmlReader( source );
        Iterator<Elements> i = t.iterator();
        Elements row = i.next();
        assertTrue( row.get( 2 ).text().equals( "R0C2" ) );
    }


    /**
     * Tests {@link HtmlReader} iterator with static file,
     */
    @Test
    public void testHtmlReaderIterator() throws HtmlReaderException {
        final Source source = Sources.file( null, file( "build/test-classes/tableOK.html" ) );
        HtmlReader t = new HtmlReader( source );
        Elements row = null;
        for ( Elements aT : t ) {
            row = aT;
        }
        assertFalse( row == null );
        assertTrue( row.get( 1 ).text().equals( "R2C1" ) );
    }


    /**
     * Tests reading a CSV file via the file adapter. Based on the test case for "NPE in planner".
     */
    @Test
    @Ignore
    public void testCsvFile() throws Exception {
        Properties info = new Properties();
        final String path = resourcePath( "sales-csv" );
        final String model = "inline:"
                + "{\n"
                + "  \"version\": \"1.0\",\n"
                + "  \"defaultSchema\": \"XXX\",\n"
                + "  \"schemas\": [\n"
                + "    {\n"
                + "      \"name\": \"FILES\",\n"
                + "      \"type\": \"custom\",\n"
                + "      \"factory\": \"org.polypheny.db.adapter.file.FileSchemaFactory\",\n"
                + "      \"operand\": {\n"
                + "        \"directory\": " + TestUtil.escapeString( path ) + "\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        info.put( "model", model );
        info.put( "lex", "JAVA" );

        try (
                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
                Statement stmt = connection.createStatement()
        ) {
            final String sql = "select * from FILES.DEPTS";
            final ResultSet rs = stmt.executeQuery( sql );
            assertThat( rs.next(), is( true ) );
            assertThat( rs.getString( 1 ), is( "10" ) );
            assertThat( rs.next(), is( true ) );
            assertThat( rs.getString( 1 ), is( "20" ) );
            assertThat( rs.next(), is( true ) );
            assertThat( rs.getString( 1 ), is( "30" ) );
            assertThat( rs.next(), is( false ) );
            rs.close();
        }
    }
}

