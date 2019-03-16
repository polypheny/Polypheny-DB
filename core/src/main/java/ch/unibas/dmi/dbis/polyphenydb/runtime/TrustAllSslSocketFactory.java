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
import java.net.InetAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


/**
 * Socket factory that trusts all SSL connections.
 */
public class TrustAllSslSocketFactory extends SocketFactoryImpl {

    private static final TrustAllSslSocketFactory DEFAULT = new TrustAllSslSocketFactory();

    private final SSLSocketFactory sslSocketFactory;


    protected TrustAllSslSocketFactory() {
        TrustManager[] trustAllCerts = { new DummyTrustManager() };
        SSLSocketFactory factory = null;
        try {
            SSLContext sc = SSLContext.getInstance( "SSL" );
            sc.init( null, trustAllCerts, new SecureRandom() );
            factory = sc.getSocketFactory();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        this.sslSocketFactory = factory;
    }


    @Override
    public Socket createSocket() throws IOException {
        return applySettings( sslSocketFactory.createSocket() );
    }


    @Override
    public Socket createSocket( InetAddress host, int port ) throws IOException {
        return applySettings( sslSocketFactory.createSocket( host, port ) );
    }


    @Override
    public Socket createSocket( InetAddress address, int port, InetAddress localAddress, int localPort ) throws IOException {
        return applySettings( sslSocketFactory.createSocket( address, port, localAddress, localPort ) );
    }


    @Override
    public Socket createSocket( String host, int port ) throws IOException {
        return applySettings( sslSocketFactory.createSocket( host, port ) );
    }


    @Override
    public Socket createSocket( String host, int port, InetAddress localHost, int localPort ) throws IOException {
        return applySettings( sslSocketFactory.createSocket( host, port, localHost, localPort ) );
    }


    /**
     * @see javax.net.SocketFactory#getDefault()
     */
    public static TrustAllSslSocketFactory getDefault() {
        return DEFAULT;
    }


    public static SSLSocketFactory getDefaultSSLSocketFactory() {
        return DEFAULT.sslSocketFactory;
    }


    /**
     * Creates an "accept-all" SSLSocketFactory - ssl sockets will accept ANY certificate sent to them - thus effectively just securing the communications. This could be set in a HttpsURLConnection using
     * HttpsURLConnection.setSSLSocketFactory(.....)
     *
     * @return SSLSocketFactory
     */
    public static SSLSocketFactory createSSLSocketFactory() {
        SSLSocketFactory sslsocketfactory = null;
        TrustManager[] trustAllCerts = { new DummyTrustManager() };
        try {
            SSLContext sc = SSLContext.getInstance( "SSL" );
            sc.init( null, trustAllCerts, new java.security.SecureRandom() );
            sslsocketfactory = sc.getSocketFactory();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
        return sslsocketfactory;
    }


    /**
     * Implementation of {@link X509TrustManager} that trusts all certificates.
     */
    private static class DummyTrustManager implements X509TrustManager {

        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }


        public void checkClientTrusted(
                X509Certificate[] certs,
                String authType ) {
        }


        public void checkServerTrusted(
                X509Certificate[] certs,
                String authType ) {
        }
    }
}

