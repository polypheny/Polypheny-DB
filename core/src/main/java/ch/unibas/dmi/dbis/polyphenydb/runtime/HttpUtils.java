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

package ch.unibas.dmi.dbis.polyphenydb.runtime;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;


/**
 * Utilities for connecting to REST services such as Splunk via HTTP.
 */
public class HttpUtils {

    private HttpUtils() {
    }


    public static HttpURLConnection getURLConnection( String url ) throws IOException {
        URLConnection conn = new URL( url ).openConnection();
        final HttpURLConnection httpConn = (HttpURLConnection) conn;

        // take care of https stuff - most of the time it's only needed to secure client/server comm
        // not to establish the identity of the server
        if ( httpConn instanceof HttpsURLConnection ) {
            HttpsURLConnection httpsConn = (HttpsURLConnection) httpConn;
            httpsConn.setSSLSocketFactory( TrustAllSslSocketFactory.createSSLSocketFactory() );
            httpsConn.setHostnameVerifier( ( arg0, arg1 ) -> true );
        }

        return httpConn;
    }


    public static void appendURLEncodedArgs( StringBuilder out, Map<String, String> args ) {
        int i = 0;
        try {
            for ( Map.Entry<String, String> me : args.entrySet() ) {
                if ( i++ != 0 ) {
                    out.append( "&" );
                }
                out.append( URLEncoder.encode( me.getKey(), "UTF-8" ) )
                        .append( "=" )
                        .append( URLEncoder.encode( me.getValue(), "UTF-8" ) );
            }
        } catch ( UnsupportedEncodingException ignore ) {
            // ignore
        }
    }


    public static void appendURLEncodedArgs( StringBuilder out, CharSequence... args ) {
        if ( args.length % 2 != 0 ) {
            throw new IllegalArgumentException( "args should contain an even number of items" );
        }
        try {
            int appended = 0;
            for ( int i = 0; i < args.length; i += 2 ) {
                if ( args[i + 1] == null ) {
                    continue;
                }
                if ( appended++ > 0 ) {
                    out.append( "&" );
                }
                out.append( URLEncoder.encode( args[i].toString(), "UTF-8" ) )
                        .append( "=" )
                        .append( URLEncoder.encode( args[i + 1].toString(), "UTF-8" ) );
            }
        } catch ( UnsupportedEncodingException ignore ) {
            // ignore
        }
    }


    public static InputStream post( String url, CharSequence data, Map<String, String> headers ) throws IOException {
        return post( url, data, headers, 10000, 60000 );
    }


    public static InputStream post( String url, CharSequence data, Map<String, String> headers, int cTimeout, int rTimeout ) throws IOException {
        return executeMethod( data == null ? "GET" : "POST", url, data, headers, cTimeout, rTimeout );
    }


    public static InputStream executeMethod( String method, String url, CharSequence data, Map<String, String> headers, int cTimeout, int rTimeout ) throws IOException {
        // NOTE: do not log "data" or "url"; may contain user name or password.
        final HttpURLConnection conn = getURLConnection( url );
        conn.setRequestMethod( method );
        conn.setReadTimeout( rTimeout );
        conn.setConnectTimeout( cTimeout );

        if ( headers != null ) {
            for ( Map.Entry<String, String> me : headers.entrySet() ) {
                conn.setRequestProperty( me.getKey(), me.getValue() );
            }
        }
        if ( data == null ) {
            return conn.getInputStream();
        }
        conn.setDoOutput( true );
        try ( Writer w = new OutputStreamWriter( conn.getOutputStream(), StandardCharsets.UTF_8 ) ) {
            w.write( data.toString() );
            w.flush(); // Get the response
            return conn.getInputStream();
        }
    }
}

