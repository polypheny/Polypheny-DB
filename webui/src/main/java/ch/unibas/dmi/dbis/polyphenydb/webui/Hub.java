/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.webui.models.HubResult;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.HubRequest;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.ConnectException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


/**
 * This class forwards REST requests to the Polypheny-DB Hub and returns the retrieved results
 */
@Slf4j
class Hub {

    private final Gson gson = new Gson();
    private final OkHttpClient httpClient = new OkHttpClient();
    private String url;

    Hub() {
        // todo use configManager
        this.url = "http://localhost:8888/Sites/unibas/polypheny/data-stores/rest.php";
    }

    HubResult login( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        //see https://www.mkyong.com/java/how-to-send-http-request-getpost-in-java/
        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "username", request.user )
                .add( "password", request.password )
                .build();

        return forward( formBody, "Login failed." );
    }

    HubResult logout( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "secret", request.secret )
                .build();

        return forward( formBody, "Logout failed." );
    }


    int checkLogin( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", String.valueOf( request.userId ) )
                .add( "secret", request.secret )
                .build();
        Request req2 = new Request.Builder().url( this.url ).post( formBody ).build();

        try ( Response response = httpClient.newCall( req2 ).execute() ) {
            return gson.fromJson( response.body().string(), Integer.class );
        } catch ( IOException | NullPointerException e ) {
            log.error( "checkLogin error", e );
            return 0;
        }
    }


    HubResult getUsers( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", String.valueOf( request.userId ) )
                .add( "secret", request.secret )
                .build();

        return forward( formBody, "Could not retrieve users." );
    }


    HubResult changePassword( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", Integer.toString( request.userId ) )
                .add( "secret", request.secret )
                .add( "oldPw", request.oldPw )
                .add( "newPw1", request.newPw1 )
                .add( "newPw2", request.newPw2 )
                .build();

        return forward( formBody, "Could not change password" );
    }


    HubResult deleteUser( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", Integer.toString( request.userId ) )
                .add( "secret", request.secret )
                .add( "deleteUser", Integer.toString( request.deleteUser ) )
                .build();

        return forward( formBody, "Could not change password" );
    }


    HubResult createUser( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", Integer.toString( request.userId ) )
                .add( "secret", request.secret )
                .add( "userName", request.name )
                .add( "admin", String.valueOf( request.admin ) )
                .add( "email", request.email )
                .build();

        return forward( formBody, "Could not change password" );
    }


    HubResult getDatasets( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", String.valueOf( request.userId ) )
                .add( "secret", request.secret )
                .build();

        return forward( formBody, "Could not retrieve datasets." );
    }


    HubResult editDataset( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", String.valueOf( request.userId ) )
                .add( "name", request.name )
                .add( "public", String.valueOf( request.pub ) )
                .build();

        return forward( formBody, "Could not edit dataset" );
    }


    HubResult uploadDataset( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", String.valueOf( request.userId ) )
                .add( "secret", request.secret )
                .add( "name", request.name )
                .add( "pub", String.valueOf( request.pub ) )
                .add( "dataset", request.dataset )
                .build();

        return forward( formBody, "Could not edit dataset" );
    }


    HubResult deleteDataset( final spark.Request req, final spark.Response res ) {
        HubRequest request = this.gson.fromJson( req.body(), HubRequest.class );

        RequestBody formBody = new FormBody.Builder()
                .add( "action", request.action )
                .add( "userId", String.valueOf( request.userId ) )
                .add( "secret", request.secret )
                .add( "datasetId", String.valueOf( request.datasetId ) )
                .build();

        return forward( formBody, "Could not edit dataset" );
    }


    private HubResult forward( final RequestBody request, final String errorMessage ) {
        Request req2 = new Request.Builder().url( this.url ).post( request ).build();

        try ( Response response = httpClient.newCall( req2 ).execute() ) {
            String body = response.body().string();
            // log.info( body );
            return gson.fromJson( body, HubResult.class );
        } catch ( ConnectException e ) {
            return new HubResult( "Could not connect to the Polypheny-DB Hub server" );
        } catch ( IOException | NullPointerException e ) {
            log.error( errorMessage, e );
            return new HubResult( errorMessage );
        }
    }

}
