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

package org.polypheny.db.polyfier.core.client;

import com.google.gson.Gson;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.polyfier.core.client.profile.Profile;

import javax.websocket.*;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.UUID;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class PolyfierClientPdb {
    private final Gson gson = new Gson();
    public final PdbWebSocketEndPoint pdbWebSocketEndPoint;
    private final PdbUpdateRoutineHandler pdbUpdateRoutineHandler;
    private final String apiKey;
    private final UUID polyfierClientId;
    private final Object jobLock = new Object();

    @Setter
    private Profile nextJob;

    public PolyfierClientPdb(URI polyfierUri, String apiKey, UUID polyfierClientId ) {
        this.apiKey = apiKey;
        this.polyfierClientId = polyfierClientId;
        this.pdbWebSocketEndPoint = new PdbWebSocketEndPoint( polyfierUri, ( message, session ) -> {

            ResponseMessage responseMessage = gson.fromJson( message, ResponseMessage.class );
            try {
                ResponseMessageCode responseMessageCode = ResponseMessageCode.valueOf( responseMessage.getMessageCode() );

                switch ( responseMessageCode ) {
                    case OK:
                        log.debug("Polyfier: OK");
                        break;
                    case JOB:
                        synchronized ( getJobLock() ) {
                            this.nextJob = getGson().fromJson( responseMessage.getBody(), Profile.class);
                            getJobLock().notify();
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("No handling for message code: " + responseMessageCode );
                }

            } catch ( IllegalArgumentException illegalArgumentException ) {
                throw new RuntimeException("Could not handle response from server.", illegalArgumentException);
            }

        } );
        this.pdbUpdateRoutineHandler = new PdbUpdateRoutineHandler(
                getPdbWebSocketEndPoint(),
                apiKey,
                getPolyfierClientId().toString(),
                "IDLE",
                true
        );
        Thread pctrlUpdateRoutineThread = new Thread( getPdbUpdateRoutineHandler() );
        pctrlUpdateRoutineThread.setDaemon( true );
        pctrlUpdateRoutineThread.start();

        this.signIn();
    }

    private void signIn() {
        getPdbWebSocketEndPoint().sendMessage( getGson().toJson(
                new PdbSignInMessage( getApiKey(), getPolyfierClientId().toString() )
        ) );
    }

    private void signOut() {
        getPdbWebSocketEndPoint().sendMessage( getGson().toJson(
                new PdbSignOutMessage( getApiKey(), getPolyfierClientId().toString() )
        ) );
    }

    public Profile requestJob() throws InterruptedException {
        if ( getNextJob() != null ) {
            setNextJob( null );
        }

        getPdbWebSocketEndPoint().sendMessage( getGson().toJson(
                new PdbReqJobMessage( getApiKey(), getPolyfierClientId().toString() )
        ) );

        synchronized ( getJobLock() ) {
            getJobLock().wait( 60000 ); // One Minute timeout. Waiting for Polyfier response to Job Request.
        }

        assert getNextJob() != null;

        return getNextJob();
    }

    public void changeStatus( String status ) {
        this.getPdbUpdateRoutineHandler().setStatus( status );
    }

    public void shutdown() {
        this.getPdbUpdateRoutineHandler().setRunning( false );
        try {
            Thread.sleep( 200 );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.signOut();
    }

    public void depositResult(
            @NonNull Long seed,
            @NonNull Boolean success,
            Long resultSetHash,
            String error,
            String logical,
            String physical,
            Long actual
    ) {
        getPdbWebSocketEndPoint().sendMessage( getGson().toJson( new PdbResultDepMessage(
                getApiKey(),
                getPolyfierClientId().toString(),
                seed,
                success,
                resultSetHash,
                error,
                logical,
                physical,
                actual
        ) ) );
    }

    public interface MessageHandler {
        void handleMessage( String message, Session session );
    }

    @ClientEndpoint
    public static class PdbWebSocketEndPoint {
        private final MessageHandler messageHandler;
        private final Session session;

        public PdbWebSocketEndPoint( URI server, MessageHandler messageHandler ) {
            WebSocketContainer webSocketContainer = ContainerProvider.getWebSocketContainer();
            try {
                this.session = webSocketContainer.connectToServer(this, server);
                this.messageHandler = messageHandler;
            } catch (DeploymentException | IOException e ) {
                throw new RuntimeException(e);
            }
        }

        public void sendMessage( String message ) {
            this.session.getAsyncRemote().sendText( message );
        }

        @OnOpen
        public void onOpen(Session session) {
            log.debug("Connected to WebSocket server: " + session.getId());
        }

        @OnMessage
        public void onMessage(String message, Session session) {
            log.debug("Received message: " + message);
            if (this.messageHandler != null) {
                this.messageHandler.handleMessage( message, session );
            } else {
                log.warn("Message was not processed, no message-handler found:" + message);
            }
        }

        @OnClose
        public void onClose(Session session) {
            log.debug("WebSocket connection closed: " + session.getId());
        }

        @OnError
        public void onError(Throwable error) {
            log.error("WebSocket error: " + error.getMessage());
        }
    }


    @AllArgsConstructor
    private static class PdbUpdateRoutineHandler implements Runnable {
        public final PdbWebSocketEndPoint pdbWebSocketEndPoint;
        private final String apiKey;
        private final String key;
        @Setter
        private String status;
        @Setter
        private boolean running;

        @Override
        public void run() {
            while ( running ) {
                try {
                    Thread.sleep( 5000 );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                pdbWebSocketEndPoint.sendMessage( new Gson().toJson( new PdbStatusUpdMessage( apiKey, key, status ) ) );

            }
        }
    }


    @Getter
    private static class PdbSignInMessage implements Serializable {
        private final String apiKey;
        private final String clientCode;
        private final String messageCode;
        private final String body;

        public PdbSignInMessage(String apiKey, String pdbKey) {
            this.apiKey = apiKey;
            this.clientCode = "PDB";
            this.messageCode = "PDB_SIGN_IN";
            this.body = new Gson().toJson( new Body(pdbKey) );
        }

        @Getter
        public static class Body implements Serializable {
            private final String key;

            public Body(String pdbKey) {
                this.key = pdbKey;
            }
        }
    }
    @Getter
    private static class PdbSignOutMessage implements Serializable {
        private final String apiKey;
        private final String clientCode;
        private final String messageCode;
        private final String body;

        public PdbSignOutMessage(String apiKey, String pdbKey) {
            this.apiKey = apiKey;
            this.clientCode = "PDB";
            this.messageCode = "PDB_SIGN_OUT";
            this.body = new Gson().toJson( new Body(pdbKey) );
        }

        @Getter
        public static class Body implements Serializable {
            private final String key;

            public Body(String pdbKey) {
                this.key = pdbKey;
            }
        }
    }


    @Getter
    private static class PdbStatusUpdMessage implements Serializable {
        private final String apiKey;
        private final String clientCode;
        private final String messageCode;
        private final String body;

        public PdbStatusUpdMessage(String apiKey, String pdbKey, String status) {
            this.apiKey = apiKey;
            this.clientCode = "PDB";
            this.messageCode = "PDB_STATUS_UPD";
            this.body = new Gson().toJson(  new Body(pdbKey, status) );
        }

        @Getter
        public static class Body implements Serializable {
            private final String key;
            private final String status;

            public Body(String pdbKey, String status) {
                this.key = pdbKey;
                this.status = status;
            }
        }
    }

    @Getter
    private static class PdbReqJobMessage implements Serializable {
        private final String apiKey;
        private final String clientCode;
        private final String messageCode;
        private final String body;

        public PdbReqJobMessage( String apiKey, String pdbKey ) {
            this.apiKey = apiKey;
            this.clientCode = "PDB";
            this.messageCode = "PDB_REQ_JOB";
            this.body = new Gson().toJson(  new Body( pdbKey ) );
        }

        @Getter
        public static class Body implements Serializable {
            private final String key;

            public Body( String pdbKey ) {
                key = pdbKey;
            }
        }
    }


    @Getter
    private static class PdbResultDepMessage implements Serializable {
        private final String apiKey;
        private final String clientCode;
        private final String messageCode;
        private final String body;

        public PdbResultDepMessage(String apiKey, String pdbKey, Long seed, Boolean success,
                                   Long resultSetHash, String error, String logical, String physical, Long actual) {
            this.apiKey = apiKey;
            this.clientCode = "PDB";
            this.messageCode = "PDB_RESULT_DEP";
            this.body = new Gson().toJson( new Body(pdbKey, seed, resultSetHash, success, error, logical, physical, actual) );
        }

        @Getter
        public static class Body implements Serializable {
            private final String pdbKey;
            private final Long seed;
            private final Long resultSetHash;
            private final Boolean success;
            private final String error;
            private final String logical;
            private final String physical;
            private final Long actual;

            public Body(String pdbKey, Long seed, Long resultSetHash, Boolean success, String error,
                        String logical, String physical, Long actual) {
                this.pdbKey = pdbKey;
                this.seed = seed;
                this.resultSetHash = resultSetHash;
                this.success = success;
                this.error = error;
                this.logical = logical;
                this.physical = physical;
                this.actual = actual;
            }
        }
    }

    @Getter
    @AllArgsConstructor
    private enum ResponseMessageCode {
        OK( null ),
        JOB( Profile.class );

        private final Class<?> clazz;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ResponseMessage implements Serializable {
        private String messageCode;
        private String body;
    }


}