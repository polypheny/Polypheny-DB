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

import ch.unibas.dmi.dbis.polyphenydb.informationprovider.*;
import ch.unibas.dmi.dbis.polyphenydb.informationprovider.InformationGraph.GraphType;
import com.google.gson.Gson;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;


/** RESTful server for the WebUis, working with the InformationManager */
public class InformationServer  {

    static {
        InformationManager im = InformationManager.getInstance();
    }

    public InformationServer() {

        port(8082);

        //needs to be called before route mapping!
        webSockets();

        enableCORS();

        informationRoutes();

        demoData();

    }

    public static void main(String[] args) {
        new InformationServer();
        System.out.println("InformationServer running..");
    }

    private void webSockets () {
        //Websockets need to be defined before the post/get requests
        webSocket("/informationWebSocket", InformationWebSocket.class);
    }

    private void informationRoutes() {
        Gson gson = new Gson();
        InformationManager im = InformationManager.getInstance();

        get("/getPageList", ( req, res ) -> im.getPageList());

        post("/getPage", (req, res) -> {
            //input: req: {pageId: "page1"}
            try{
                //System.out.println("get page "+req.body());
                return im.getPage( req.body() );
            } catch ( Exception e ){
                //if input not number or page does not exist
                return "";
            }
        });

    }

    /** to avoid the CORS problem, when the ConfigServer receives requests from the WebUi */
    private static void enableCORS() {
        ConfigServer.enableCORS();
    }

    /** just for testing */
    private void demoData() {
        InformationManager im = InformationManager.getInstance();

        InformationPage p = new InformationPage( "page1", "Page 1" );
        InformationGroup g1 = new InformationGroup( "group1.1", "page1" );
        InformationGroup g2 = new InformationGroup( "group1.2", "page1" );
        im.addPage( p );
        im.addGroup( g1, g2 );

        Information i1 = new InformationHeader( "i.header", "group1.1", "Gruppe 1" );
        Information i2 = new InformationProgress( "i.progress", "group1.1", "progval", 70 ).setColor( "" );
        //Information i3 = new InformationCollapsible( "i.collapse", "group1.1", "myCollapsible", i1, i2 );
        Information i4 = new InformationHtml( "i.html" , "group1.1", "<b>bold</b>");

        String[] labels = {"Jan", "Feb", "März", "April", "Mail", "Juni"};
        int[] graphData1 = {5,2,7,3,2,1};
        int[] graphData2 = {7,8,2,2,7,3};
        GraphData[] graphData = {new GraphData( "data1", graphData1 ), new GraphData( "data2", graphData2 )};
        Information i5 = new InformationGraph( "i.graph" , "group1.1", labels, graphData );
        Information i6 = new InformationGraph( "i.graph2", "group1.2", labels, graphData ).ofType( GraphType.BAR );

        im.regsiterInformation( i1, i2, i4, i5, i6 );

        Timer timer = new Timer();
        timer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                Random r = new Random();
                im.getInformation( "i.progress" ).updateProgress( r.nextInt(100) );
            }
        }, 5000, 5000 );
    }

}
