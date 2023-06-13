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

package org.polypheny.db.jupyter;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.jupyter.JupyterClient.JupyterServerException;
import org.polypheny.db.webui.Crud;
import io.javalin.http.Context;

@Slf4j
public class JupyterProxy {

    private final JupyterClient client;
    private final Gson gson = new Gson();


    public JupyterProxy( JupyterClient client ) {
        this.client = client;
    }


    // ============= GET =============
    public void kernelspecs( final Context ctx, Crud crud ) {
        forward( ctx, client::getKernelspecs );
    }


    public void session( final Context ctx, Crud crud ) {
        String sessionId = ctx.pathParam( "sessionId" );
        forward( ctx, () -> client.getSession( sessionId ) );

    }


    public void sessions( final Context ctx, Crud crud ) {
        forward( ctx, client::getSessions );
    }


    public void contents( final Context ctx, Crud crud ) {
        String content = ctx.queryParam( "content" );
        String format = ctx.queryParam( "format" );
        String path = ctx.pathParam( "path" );
        forward( ctx, () -> client.getContents( path, content == null ? "1" : content, format ) );
    }


    public void file(final Context ctx, Crud crud) {
        String path = ctx.pathParam( "path" );
        try {
            HttpResponse<String> response = client.getFileBase64( path );
            JsonObject body = gson.fromJson( response.body(), JsonObject.class );
            String base64 = body.get( "content" ).getAsString().replace( "\n", "" );
            byte[] data = Base64.getDecoder().decode(base64.getBytes( StandardCharsets.UTF_8));
            String mimetype = body.get("mimetype").getAsString();
            ctx.contentType(mimetype);
            ctx.result( data );

        } catch ( JupyterServerException e ) {
            ctx.status( e.getStatus() ).result(e.getMsg());
        } catch ( IllegalArgumentException e ) {
            ctx.status(404).result("Invalid file");
        }

    }


    public void kernels( final Context ctx, Crud crud ) {
        forward( ctx, client::getRunningKernels );
    }


    // ============= POST =============
    public void createFile( final Context ctx, Crud crud ) {
        String parentPath = ctx.pathParam( "parentPath" );
        String body = ctx.body();
        forward( ctx, () -> client.createFile( parentPath, body ) );
    }


    public void createSession( final Context ctx, Crud crud ) {
        String body = ctx.body();
        forward( ctx, () -> client.createSession( body ) );
    }


    public void interruptKernel( final Context ctx, Crud crud ) {
        String kernelId = ctx.pathParam( "kernelId" );
        forward( ctx, () -> client.interruptKernel( kernelId ) );
    }


    public void restartKernel( final Context ctx, Crud crud ) {
        String kernelId = ctx.pathParam( "kernelId" );
        forward( ctx, () -> client.restartKernel( kernelId ) );
    }


    // ============= PATCH =============
    public void patchSession( final Context ctx, Crud crud ) {
        String sessionId = ctx.pathParam( "sessionId" );
        String body = ctx.body();
        forward( ctx, () -> client.patchSession( sessionId, body ) );
    }


    public void moveFile( final Context ctx, Crud crud ) {
        String filePath = ctx.pathParam( "filePath" );
        String body = ctx.body();
        forward( ctx, () -> client.moveFile( filePath, body ) );
    }


    // ============= PUT =============
    public void uploadFile( final Context ctx, Crud crud ) {
        String filePath = ctx.pathParam( "filePath" );
        String body = ctx.body();
        forward( ctx, () -> client.putFile( filePath, body ) );
    }


    // ============= DELETE =============
    public void deleteSession( final Context ctx, Crud crud ) {
        String sessionId = ctx.pathParam( "sessionId" );
        forward( ctx, () -> client.deleteSession( sessionId ) );
    }


    public void deleteFile( final Context ctx, Crud crud ) {
        String filePath = ctx.pathParam( "filePath" );
        forward( ctx, () -> client.deleteFile( filePath ) );
    }


    private void forward( Context ctx, HttpResponseSupplier supplier ) {
        ctx.contentType( "application/json" );
        try {
            HttpResponse<String> res = supplier.get();
            log.info( "Responding with: {}", res.body() );
            ctx.status( res.statusCode() );
            ctx.result( res.body() );

        } catch ( JupyterServerException e ) {
            ctx.status( e.getStatus() ).result(e.getMsg());
        }
    }


    @FunctionalInterface
    private interface HttpResponseSupplier {

        HttpResponse<String> get() throws JupyterServerException;

    }

}
