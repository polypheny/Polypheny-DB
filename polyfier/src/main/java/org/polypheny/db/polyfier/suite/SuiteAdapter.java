/*
 * Copyright 2019-2023 The Polypheny Project
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
 */

package org.polypheny.db.polyfier.suite;


import java.io.*;
import java.net.*;
import java.util.Optional;

import com.google.gson.*;
import org.polypheny.db.polyfier.suite.requests.QueryRequest;
import org.polypheny.db.polyfier.suite.requests.SuiteRequest;
import org.polypheny.db.polyfier.suite.responses.InfoResponse;
import org.polypheny.db.polyfier.suite.responses.SuiteResponse;


@Deprecated
public class SuiteAdapter implements Runnable {
    private static final int R_HEADER = 16;
    private static final int R_SIZE = 10;
    private static final int R_CODE = 6;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private Gson gson;
    private int count;
    private int size;
    private String code;
    private char[] buff1 = new char[ R_HEADER ];
    private char[] buff2 = new char[ R_CODE ];
    private char[] buff3;

    public SuiteAdapter( int port ) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.gson = new Gson();
    }

    public void run() {
        while ( true ) {
            listen();
            try {
                out.flush();
                this.in.close();
                this.out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void listen() {
        System.out.println( "Listening..." );
        try {
            this.clientSocket = serverSocket.accept();
            this.out = new PrintWriter( clientSocket.getOutputStream(), true );
            this.in = new BufferedReader( new InputStreamReader( clientSocket.getInputStream() ) );

            buff1 = new char[ R_SIZE ];
            buff2 = new char[ R_CODE ];

            count = in.read( buff1 );
            if ( count == -1 ) {
                out.write( prep( InfoResponse.invalidRequest( "Request too short." ) ) );
                return;
            }

            try {
                size = Integer.parseInt( new String( buff1 ) );
            } catch ( NumberFormatException numberFormatException ) {
                out.write( prep( InfoResponse.invalidRequest( "Illegal characters in the request header." ) ) );
                return;
            }

            count = in.read( buff2 );

            if ( count == -1 ) {
                out.write( prep( InfoResponse.invalidRequest( "Request too short." ) ) );
                return;
            }

            code = new String( buff2 );

            buff3 = new char[size];

            count = in.read( buff3 );

            if ( count != size ) {
                out.write( prep( InfoResponse.invalidRequest( "Specified request size (" + size + "bytes) is bigger than actual size." ) ) );
                return;
            }

            // ---------------------------------------------------------------------
            // Request Handling

            Optional<SuiteRequest> request;
            switch ( code ) {
                case SuiteComm.QUERY_CODE:
                    request = read( buff3, QueryRequest.class );
                    request.ifPresentOrElse(
                            r -> handleQueryRequest( (QueryRequest) r),
                            this::answerNotOk
                    );
                    return;
                default:
                    out.write( prep( InfoResponse.invalidRequest( "We don't have any request type matching this code." )));
            }

            // ---------------------------------------------------------------------

        } catch (IOException ioException) {
            System.out.println("Terminated Connection...");
        }
    }

    private void handleQueryRequest( QueryRequest queryRequest ) {
        // Todo
        // Stub
        answerOk();
    }

    private void answerOk() {
        out.write( prep( InfoResponse.ok() ) );
    }

    private void answerNotOk() {
        out.write( prep( InfoResponse.invalidRequest( "Could not parse JSON according to code." ) ) );
    }


    private char[] prep( SuiteResponse suiteResponse ) {
        String s = this.gson.toJson( suiteResponse );

        String prefix = String.valueOf(s.length());

        if ( prefix.length() > R_SIZE ) {
            throw new IllegalArgumentException("The message cannot be larger than a gigabyte.");
        }
        if ( prefix.length() == 0 ) {
            throw new IllegalArgumentException("The message cannot be empty.");
        }

        return ("0".repeat( R_SIZE - ( prefix.length() % R_SIZE ) ) + prefix + suiteResponse.getCode() + s).toCharArray();
    }

    private Optional<SuiteRequest> read( char[] data, Class<? extends SuiteRequest> clazz ) {
        String s = new String( data );
        try {
            return Optional.of( this.gson.fromJson( s, clazz ) );
        } catch ( JsonSyntaxException jsonSyntaxException ) {
            jsonSyntaxException.printStackTrace();
            return Optional.empty();
        }
    }


    public static void main(String[] args) {
        try {
            SuiteAdapter suiteAdapter = new SuiteAdapter(6666);
            suiteAdapter.run();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
