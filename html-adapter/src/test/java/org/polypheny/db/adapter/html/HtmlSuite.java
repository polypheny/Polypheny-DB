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


import java.io.IOException;
import java.net.Socket;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


/**
 * Unit test suite for Polypheny-DB Html adapter.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ HtmlReaderTest.class, SqlTest.class })
public class HtmlSuite {

    private HtmlSuite() {
    }


    private static final String TEST_HOST = "en.wikipedia.org";


    static boolean hazNetwork() {
        Socket socket = null;
        boolean reachable = false;
        try {
            socket = new Socket( HtmlSuite.TEST_HOST, 80 );
            reachable = true;
        } catch ( Exception e ) {
            // do nothing
        } finally {
            if ( socket != null ) {
                try {
                    socket.close();
                } catch ( IOException e ) {
                    // do nothing
                }
            }
        }
        return reachable;
    }

}

