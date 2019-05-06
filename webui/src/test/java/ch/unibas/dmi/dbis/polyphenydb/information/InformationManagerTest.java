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
import org.junit.Assert;
import org.junit.Test;


public class InformationManagerTest {

    private InformationManager im;


    static {
        InformationManager im = InformationManager.getInstance();

        InformationPage p = new InformationPage( "page1", "Page 1" );
        InformationGroup g = new InformationGroup( "group1.1", "page1" );
        im.addPage( p );
        im.addGroup( g );

        Information i1 = new InformationProgress( "i.progress", "group1.1", "progval", 30 );
        Information i2 = new InformationHtml( "i.html", "group1.1", "<b>bold</b>" );

        im.registerInformation( i1, i2 );
    }


    public InformationManagerTest() {
        this.im = InformationManager.getInstance();
    }


    @Test
    public void getPage() {
        System.out.println( this.im.getPage( "page1" ) );
        //Gson gson = new Gson();
        //InformationPage p = gson.fromJson( this.im.getPage( "page1" ), InformationPage.class );
    }


    @Test
    public void getPageList() {
        System.out.println( this.im.getPageList() );
    }


    @Test
    public void informationType() {
        Information i1 = new InformationHtml( "id", "group", "html" );
        Assert.assertEquals( "InformationHtml", i1.type );
    }


    @Test(expected = RuntimeException.class)
    public void graphThrowingError() {
        String[] labels = { "Jan", "Feb", "März", "April", "Mail", "Juni" };
        Integer[] graphData1 = { 5, 2, 7, 3, 2, 1 };
        Integer[] graphData2 = { 7, 8, 2, 2, 7, 3 };
        GraphData[] graphData = { new GraphData<Integer>( "data1", graphData1 ), new GraphData<Integer>( "data2", graphData2 ) };
        Information i1 = new InformationGraph( "i.graph", "group1.1", GraphType.PIE, labels, graphData );
    }


    @Test
    public void changeGraphType() {
        String[] labels = { "Jan", "Feb", "März", "April", "Mail", "Juni" };
        Integer[] graphData1 = { 5, 2, 7, 3, 2, 1 };
        Integer[] graphData2 = { 7, 8, 2, 2, 7, 3 };
        GraphData[] graphData = { new GraphData<Integer>( "data1", graphData1 ), new GraphData<Integer>( "data2", graphData2 ) };
        InformationGraph i1 = new InformationGraph( "i.graph", "group1.1", GraphType.LINE, labels, graphData );
        i1.updateType( GraphType.RADAR );
    }


}
