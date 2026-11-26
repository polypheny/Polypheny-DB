/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.monetdb.stores;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import org.junit.jupiter.api.Test;


class MonetdbStoreTest {

    @Test
    void dockerProbeTimesOutWhileWaitingForGreeting() throws Exception {
        Class.forName( "nl.cwi.monetdb.jdbc.MonetDriver" );
        String host = "127.0.0.1";
        // Leave the socket listening without sending a MAPI greeting.
        try ( ServerSocket silentServer = new ServerSocket( 0, 1, InetAddress.getByName( host ) ) ) {
            int port = silentServer.getLocalPort();
            String probeUrl = MonetdbStore.getConnectionUrl( host, port, "monetdb", true );
            SQLException failure = assertTimeoutPreemptively( Duration.ofSeconds( 5 ),
                    () -> assertThrows( SQLException.class, () -> DriverManager.getConnection( probeUrl, "monetdb", "unused" ) ) );
            assertTrue( failure.getMessage().contains( "Read timed out" ), failure::getMessage );
            assertEquals( "jdbc:monetdb://" + host + ":" + port + "/monetdb", MonetdbStore.getConnectionUrl( host, port, "monetdb", false ) );
        }
    }

}
