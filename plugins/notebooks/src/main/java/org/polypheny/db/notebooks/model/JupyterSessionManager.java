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

package org.polypheny.db.notebooks.model;

import java.net.http.WebSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.transaction.TransactionManager;

/**
 * The JupyterSessionManager is a singleton class that mirrors the state of sessions and kernels of the jupyter server.
 * Every running kernel has a corresponding JupyterKernel instance and open websocket connection.
 */
public class JupyterSessionManager {

    private static JupyterSessionManager INSTANCE = null;
    private final Map<String, JupyterKernel> kernels = new ConcurrentHashMap<>();
    private final Map<String, JupyterSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, JupyterKernelSpec> kernelSpecs = new ConcurrentHashMap<>();

    @Getter
    @Setter
    private String defaultKernel;

    @Getter
    @Setter
    private TransactionManager transactionManager;


    public static JupyterSessionManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new JupyterSessionManager();
        }
        return INSTANCE;
    }


    private JupyterSessionManager() {

    }


    /**
     * Creates a new JupyterKernel, if no kernel with the same id already exists.
     * Otherwise, this has no effect.
     *
     * @param kernelId the kernel ID
     * @param name the unique name of the kernel
     * @param builder a Websocket.Builder used to open a websocket to the kernel
     * @param host the address to the Docker container running the jupyter server
     */
    public void addKernel( String kernelId, String name, WebSocket.Builder builder, String host ) {
        kernels.computeIfAbsent( kernelId, k -> new JupyterKernel( kernelId, name, builder, host ) );
    }


    /**
     * Updates the specified session with the given values.
     * If no session with the specified sessionId exists, a new one is created.
     * If no kernel with the specified kernelId is known, the session is removed.
     *
     * @param sessionId the ID of the session
     * @param name the name of the session
     * @param path the path to the session
     * @param kernelId the ID of the kernel used by this session
     */
    public void updateSession( String sessionId, String name, String path, String kernelId ) {
        JupyterKernel kernel = kernels.get( kernelId );
        if ( kernel == null ) {
            sessions.remove( sessionId );
            return;
        }
        JupyterSession session = sessions.get( sessionId );
        if ( session == null ) {
            sessions.put( sessionId, new JupyterSession( sessionId, name, path, kernel ) );
            return;
        }
        session.setName( name );
        session.setPath( path );
        session.setKernel( kernel );
    }


    /**
     * Removes the session with the given session id, its associated kernel and all other sessions using that kernel.
     *
     * @param sessionId the ID of the session to be removed
     */
    public void invalidateSession( String sessionId ) {
        JupyterSession session = sessions.remove( sessionId );
        if ( session != null ) {
            removeKernel( session.getKernel().getKernelId() );
            removeSessionsWithInvalidKernels();
        }

    }


    public void addKernelSpec( String name, String displayName, String language ) {
        kernelSpecs.computeIfAbsent( name, k -> new JupyterKernelSpec( name, displayName, language ) );
    }


    /**
     * Removes all sessions that are not in the specified Set.
     *
     * @param validSessionIds a Set containing the IDs of all sessions to be retained.
     */
    public void retainValidSessions( Set<String> validSessionIds ) {
        sessions.keySet().retainAll( validSessionIds );
    }


    /**
     * Removes all kernels that are not in the specified Set.
     *
     * @param validKernelIds a Set containing the IDs of all kernels to be retained.
     */
    public void retainValidKernels( Set<String> validKernelIds ) {
        for ( String id : kernels.keySet() ) {
            if ( !validKernelIds.contains( id ) ) {
                removeKernel( id );
            }
        }
    }


    /**
     * Removes all sessions that have no valid kernel associated with them.
     */
    public void removeSessionsWithInvalidKernels() {
        sessions.entrySet().removeIf( entry -> !kernels.containsKey( entry.getValue().getKernel().getKernelId() ) );
    }


    public void removeKernel( String kernelId ) {
        JupyterKernel kernel = kernels.remove( kernelId );
        if ( kernel != null ) {
            kernel.close();
        }
    }


    public JupyterKernel getKernel( String kernelId ) {
        return kernels.get( kernelId );
    }


    public JupyterKernel getKernelFromSession( String sessionId ) {
        JupyterSession session = sessions.get( sessionId );
        if ( session != null ) {
            return session.getKernel();
        }
        return null;
    }


    /**
     * Returns for each kernel the number of UI instances that are currently connected to it.
     *
     * @return A mapping of kernelIds to the number of UI instances connected to the corresponding JupyterKernel
     */
    public Map<String, Integer> getOpenConnectionCount() {
        Map<String, Integer> openConnections = new HashMap<>();
        for ( JupyterKernel kernel : kernels.values() ) {
            openConnections.put( kernel.getKernelId(), kernel.getSubscriberCount() );
        }
        return openConnections;
    }


    /**
     * Get a string representation of the current state of stored sessions and kernels.
     * This can be useful for debugging
     *
     * @return A string listing all sessions, kernels and kernelspecs
     */
    public String getOverview() {
        // just for testing
        StringBuilder sb = new StringBuilder( "JupyterSessionManager Overview:\n" );
        sb.append( "Sessions:\n\t" );
        for ( JupyterSession session : sessions.values() ) {
            sb.append( "id: " ).append( session.getSessionId() )
                    .append( "    name: " ).append( session.getName() )
                    .append( "    path: " ).append( session.getPath() )
                    .append( "    kernel: " ).append( session.getKernel().getKernelId() )
                    .append( "\n\t" );
        }
        sb.append( "\nRunning Kernels:\n\t" );
        for ( JupyterKernel kernel : kernels.values() ) {
            sb.append( "id: " ).append( kernel.getKernelId() )
                    .append( "    client_id: " ).append( kernel.getClientId() )
                    .append( "    name: " ).append( kernel.getName() )
                    .append( "    status: " ).append( kernel.getStatus() )
                    .append( "\n\t" );
        }
        sb.append( "\nAvailable Kernels:\n\t" );
        for ( JupyterKernelSpec spec : kernelSpecs.values() ) {
            sb.append( "name: " ).append( spec.name )
                    .append( "    display_name: " ).append( spec.displayName )
                    .append( "    language: " ).append( spec.language )
                    .append( "\n\t" );
        }
        sb.append( "\nDefault Kernel: " ).append( defaultKernel ).append( "\n" );
        return sb.toString();
    }


    /**
     * Remove all sessions and kernels. All websockets to the jupyter server are closed.
     */
    public void reset() {
        sessions.clear();
        kernels.forEach( ( id, kernel ) -> kernel.close() );
        kernels.clear();
    }

}
