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

package ch.unibas.dmi.dbis.polyphenydb.information;


import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphData;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphType;
import ch.unibas.dmi.dbis.polyphenydb.webui.InformationServer;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;


public class InformationServerTest {


    public static void main( String[] args ) {
        InformationServer s = new InformationServer();
        demoData();
    }


    /**
     * Generate test data
     */
    private static void demoData() {
        InformationManager im = InformationManager.getInstance();

        InformationPage p = new InformationPage( "page1", "Page 1", "Description of page 1." );
        InformationGroup g1 = new InformationGroup( "group1.1", "page1" );
        InformationGroup g2 = new InformationGroup( "group1.2", "page1" ).setOrder( 1 );
        im.addPage( p );
        im.addGroup( g1, g2 );

        Information i2 = new InformationProgress( "i.progress", "group1.1", "progval", 70 ).setColor( ProgressColor.DYNAMIC );
        Information i3 = new InformationHtml( "i.html", "group1.1", "<b>bold</b>" ).setOrder( 2 );

        String[] labels = { "Jan", "Feb", "März", "April", "Mail", "Juni" };
        int[] graphData1 = { 5, 2, 7, 3, 2, 1 };
        int[] graphData2 = { 7, 8, 2, 2, 7, 3 };
        GraphData[] graphData = { new GraphData( "data1", graphData1 ), new GraphData( "data2", graphData2 ) };
        Information i5 = new InformationGraph( "i.graph", "group1.1", labels, graphData );
        Information i6 = new InformationGraph( "i.graph2", "group1.2", labels, graphData ).setType( GraphType.BAR );

        InformationGroup g3 = new InformationGroup( "group1.3", "page1" );
        InformationGroup g4 = new InformationGroup( "group1.4", "page1" ).setColor( GroupColor.LIGHTBLUE );

        Information h3 = new InformationHtml( "html1", "group1.3", "Test of <b>html</b>" );
        Information h4 = new InformationHtml( "html2", "group1.4", "Test of <i>html</i>" );
        Information h5 = new InformationLink( "h.link", "group1.3", "config", "/config" );
        im.registerInformation( h3, h4, h5 );
        im.addGroup( g3, g4 );

        im.registerInformation( i2, i3, i5, i6 );

        System.out.println( im.getInformation( "i.progress" ) );

        Timer timer = new Timer();
        timer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                Random r = new Random();
                im.getInformation( "i.progress" ).unwrap( InformationProgress.class ).updateProgress( r.nextInt( 100 ) );
            }
        }, 1000, 1000 );
    }


}
