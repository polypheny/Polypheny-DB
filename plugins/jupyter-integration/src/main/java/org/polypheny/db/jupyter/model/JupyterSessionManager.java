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

package org.polypheny.db.jupyter.model;

import java.net.http.WebSocket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;

public class JupyterSessionManager {

    private static JupyterSessionManager INSTANCE = null;

    private final Map<String, JupyterKernel> kernels = new ConcurrentHashMap<>();
    private final Map<String, JupyterSession> sessions = new ConcurrentHashMap<>();

    private final Map<String, JupyterKernelSpec> kernelSpecs = new ConcurrentHashMap<>();

    @Getter
    @Setter
    private String defaultKernel;


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
     */
    public void addKernel( String kernelId, String name, WebSocket.Builder builder ) {
        kernels.computeIfAbsent( kernelId, k -> new JupyterKernel( kernelId, name, builder ) );
    }


    /**
     * Updates the specified session with the given values.
     * If no session with the specified sessionId exists, a new one is created.
     * If no kernel with the specified kernelId is known, the session is removed.
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


    public void addKernelSpec( String name, String displayName, String language ) {
        kernelSpecs.computeIfAbsent( name, k -> new JupyterKernelSpec( name, displayName, language ) );
    }


    public void retainValidSessions( Set<String> validSessionIds ) {
        sessions.keySet().retainAll( validSessionIds );
    }


    public void retainValidKernels( Set<String> validKernelIds ) {
        kernels.keySet().retainAll( validKernelIds );
        removeSessionsWithInvalidKernels();
    }


    public void removeSessionsWithInvalidKernels() {
        sessions.entrySet().removeIf( entry -> !kernels.containsKey( entry.getValue().getKernel().getKernelId() ) );
    }


    public void removeKernel( String kernelId ) {
        sessions.entrySet().removeIf( entry -> entry.getValue().isKernel( kernelId ) );
        kernels.remove( kernelId );
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

}
