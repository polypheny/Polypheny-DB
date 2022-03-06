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

package org.polypheny.db.webui;


import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;


public class WebsocketTest {

    @Test
    public void invokeSqlCommand() throws InterruptedException, URISyntaxException {

        String command = "{\"requestType\":\"QueryRequest\",\"query\":\"insert into HIGH_PAYED2 (name) values ('Tedd')\",\"analyze\":true,\"cache\":true,\"language\":\"sql\",\"database\":\"private\"}";

        final WebsocketClientEndpoint clientEndPoint = new WebsocketClientEndpoint(new URI("ws://localhost:8080/webSocket"));

        // add listener
        clientEndPoint.addMessageHandler(System.out::println);

        // send message to websocket
        clientEndPoint.sendMessage(command);

        // wait 5 seconds for messages from websocket
        Thread.sleep(5000);
    }

}
