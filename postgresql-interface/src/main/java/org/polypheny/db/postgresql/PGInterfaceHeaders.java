/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.postgresql;


/**
 * All the different header, alphabetically and sender ordered.
 * There exist some special cases (as numbers as header, or no header)
 * For more information look at the Postgresql documentation: https://www.postgresql.org/docs/current/protocol-message-formats.html
 */
//TODO: not complete with all commands yet (especially for copy commands, extended query cycle and different authentication methods)
public enum PGInterfaceHeaders {

    //----------------------------------------------- server to client ------------------------------------------------

    /**
     * CommandComplete - from server to client
     */
    C,

    /**
     * DataRow - from server to client - identifies message as a data row
     */
    D,

    /**
     * ErrorResponse - from server to client - message is error
     * Execute - from client to server - used in extended query cycle
     */
    E,

    /**
     * EmptyQueryResponse - from server to client - response to empty query String  substitutes for CommandComplete
     */
    I,

    /**
     * NoticeResponse - from server to client
     */
    N,

    /**
     * no data indicator - from server to client - indicator
     */
    n,

    /**
     * Authenticatio request  - from server to client - used by several different messages
     */
    R,

    /**
     * RowDescription - from server to client
     */
    T,

    /**
     * ParameterDescription - from server to client
     */
    t,

    /**
     * ReadyForQuery - from server to client - whenever backend is ready for new query cycle
     */
    Z,

    /**
     * ParseComplete - from server to client - indicator (actually sent as 1)
     */
    //TODO: does it make sense to use the word?
    ONE,

    //----------------------------------------------- client to server ------------------------------------------------

    //E - also used from server to client, described there
    //some messages with no header (StartUpMessage, CancelREquest, and some authentication requests)

    /**
     * Simple Query -  from client to server - used in simple query cycle
     */
    Q,

    /**
     * Termination message - from client to server
     */
    X

}

/*
ONE(1);

    private final int value;

    Headers(final int newVal) {
        value = newVal;
    }

    public int getValue() { return value; }
 */
