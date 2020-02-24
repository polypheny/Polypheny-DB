/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.information;


import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;


public class InformationManagerTest {

    private InformationManager im;

    private static InformationPage p = new InformationPage( "page1", "Page 1" );
    private static InformationGroup g = new InformationGroup( p, "Group 1.1" );


    static {
        InformationManager im = InformationManager.getInstance();

        im.addPage( p );
        im.addGroup( g );

        Information i1 = new InformationProgress( g, "progval", 30 );
        Information i2 = new InformationHtml( g, "<b>bold</b>" );

        im.registerInformation( i1, i2 );
    }


    public InformationManagerTest() {
        this.im = InformationManager.getInstance();
    }


    @Test
    public void getPage() {
        System.out.println( this.im.getPage( "page1" ).asJson() );
        //Gson gson = new Gson();
        //InformationPage p = gson.fromJson( this.im.getPage( "page1" ).asJson(), InformationPage.class );
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
        GraphData[] graphData = { new GraphData<>( "data1", graphData1 ), new GraphData<>( "data2", graphData2 ) };
        Information i1 = new InformationGraph( g, GraphType.PIE, labels, graphData );
    }


    @Test
    public void changeGraphType() {
        String[] labels = { "Jan", "Feb", "März", "April", "Mail", "Juni" };
        Integer[] graphData1 = { 5, 2, 7, 3, 2, 1 };
        Integer[] graphData2 = { 7, 8, 2, 2, 7, 3 };
        GraphData[] graphData = { new GraphData<>( "data1", graphData1 ), new GraphData<>( "data2", graphData2 ) };
        InformationGraph i1 = new InformationGraph( g, GraphType.LINE, labels, graphData );
        i1.updateType( GraphType.RADAR );
    }

    @Test
    public void informationAction() {
        final int[] a = { 1 };
        InformationAction action = new InformationAction( g, "btnLabel", () -> {
            a[0]++;
            return String.valueOf( a[0] );
        } );
        action.executeAction();
        Assert.assertEquals( a[0], 2 );
    }

}
