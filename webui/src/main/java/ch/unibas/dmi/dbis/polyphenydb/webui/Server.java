/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;

import static spark.Spark.*;
import spark.Filter;
import com.google.gson.Gson;
import spark.Request;
import spark.Response;


public class Server {

    static {
        ConfigManager.getInstance().registerConfig( new Config<Integer>( "server.test", "just for testing" ).requiresRestart() );
    }

    public Server() {

        port(8081);

        enableCORS( "*", "*", "*" );

        post("/register", (req, res) -> {
            res.type("application/json");
            Gson gson = new Gson();
            int[] test = {3,6,1};
            System.out.println(gson.toJson( test ));
            return gson.toJson( test );
        });

        get("/updateTest", (req, res) -> {
            res.type("application/json");
            ConfigManager.getInstance().set( "server.test", 10);
            return 10;
        });

        get("/getTest", (req, res) -> ConfigManager.getInstance().getInt("server.test"));

        get("/getConfig", (req, res) -> ConfigManager.getInstance().getConfig( "server.test" ).toString());

    }

    public static void main(String[] args) {
        new Server();

        System.out.println("server running..");
    }

    // https://yobriefca.se/blog/2014/02/20/spas-and-enabling-cors-in-spark/
    private static void enableCORS(final String origin, final String methods, final String headers) {
        before(new Filter() {
            @Override
            public void handle( Request request, Response response) {
                response.header("Access-Control-Allow-Origin", origin);
                response.header("Access-Control-Request-Method", methods);
                response.header("Access-Control-Allow-Headers", headers);
            }
        });
    }


}
