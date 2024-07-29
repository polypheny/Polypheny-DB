/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.notebooks;

import com.google.gson.Gson;
import io.javalin.http.Context;
import java.net.http.HttpResponse;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.notebooks.JupyterClient.JupyterServerException;
import org.polypheny.db.notebooks.model.JupyterSessionManager;

@Slf4j
public class JupyterProxy {

    @Setter
    private JupyterClient client;
    private final Gson gson = new Gson();


    public JupyterProxy( JupyterClient client ) {
        this.client = client;
    }


    // ============= GET =============
    public void kernelspecs( final Context ctx ) {
        forward( ctx, client::getKernelspecs );
    }


    public void session( final Context ctx ) {
        String sessionId = ctx.pathParam( "sessionId" );
        forward( ctx, () -> client.getSession( sessionId ) );

    }


    public void sessions( final Context ctx ) {
        forward( ctx, client::getSessions );
    }


    public void kernels( final Context ctx ) {
        forward( ctx, client::getRunningKernels );
    }


    public void connectionStatus( final Context ctx ) {
        forward( ctx, client::getStatus );
    }


    public void openConnections( final Context ctx ) {
        ctx.json( JupyterSessionManager.getInstance().getOpenConnectionCount() );
    }


    // ============= POST =============
    public void createSession( final Context ctx ) {
        String body = ctx.body();
        forward( ctx, () -> client.createSession( body ) );
    }


    public void interruptKernel( final Context ctx ) {
        String kernelId = ctx.pathParam( "kernelId" );
        forward( ctx, () -> client.interruptKernel( kernelId ) );
    }


    public void restartKernel( final Context ctx ) {
        String kernelId = ctx.pathParam( "kernelId" );
        forward( ctx, () -> client.restartKernel( kernelId ) );
    }


    // ============= PATCH =============
    public void patchSession( final Context ctx ) {
        String sessionId = ctx.pathParam( "sessionId" );
        String body = ctx.body();
        forward( ctx, () -> client.patchSession( sessionId, body ) );
    }

    // ============= PUT =============


    // ============= DELETE =============
    public void deleteSession( final Context ctx ) {
        String sessionId = ctx.pathParam( "sessionId" );
        forward( ctx, () -> client.deleteSession( sessionId ) );
    }


    /**
     * Forwards a request from the UI to the jupyter server and sends back the result.
     * If JupyterServerException occurs, the details of the exception are returned to the UI.
     *
     * @param ctx the Context for replying to the UI
     * @param supplier the HttpResponseSupplier that returns the response from the jupyter server.
     */
    private void forward( Context ctx, HttpResponseSupplier supplier ) {
        ctx.contentType( "application/json" );
        try {
            HttpResponse<String> res = supplier.get();
            ctx.status( res.statusCode() );
            ctx.result( res.body() );

        } catch ( JupyterServerException e ) {
            ctx.status( e.getStatus().getCode() ).result( e.getMsg() );
        }
    }


    /**
     * Represents a function that returns a HttpResponse when called and might throw a JupyterServerException.
     */
    @FunctionalInterface
    private interface HttpResponseSupplier {

        HttpResponse<String> get() throws JupyterServerException;

    }

}
