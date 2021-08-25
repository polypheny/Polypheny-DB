/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.webui;

import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationHtml;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationProgress;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.information.InformationText;

public class UiTestingMonitoringPage {

    private static final InformationPage p = new InformationPage( "page1", "MonitoringTestPage", "Test Page 1" );
    private static final InformationGroup g = new InformationGroup( p, "Group 1" );
    private static final InformationGroup g2 = new InformationGroup( p, "Group 2" );


    static {
        InformationManager im = InformationManager.getInstance();

        im.addPage( p );
        im.addGroup( g );
        im.addGroup( g2 );

        Information i1 = new InformationHtml( g, "<b>BOLD HTML TEXT HERE</b>" );
        final String[] test = { "", "" };
        Information i2 = new InformationAction( g, "THIS IS A BUTTON", ( params ) -> {
            test[0] = "a";
            test[1] = params.get( "param1" );
            return "done";
        } );

        String[] labels = { "Jan", "Feb", "März", "April", "Mail", "Juni" };
        Integer[] graphData1 = { 5, 2, 7, 3, 2, 1 };
        Integer[] graphData2 = { 7, 8, 2, 2, 7, 3 };

        InformationGraph.GraphData[] graphData = { new InformationGraph.GraphData<>( "data1", graphData1 ), new InformationGraph.GraphData<>( "data2", graphData2 ) };
        Information i3 = new InformationGraph( g, InformationGraph.GraphType.LINE, labels, graphData );

        InformationGraph.GraphData[] graphDatatoo = { new InformationGraph.GraphData<>( "data1", graphData1 ) };
        Information i4 = new InformationGraph( g2, InformationGraph.GraphType.PIE, labels, graphDatatoo );

        Information i5 = new InformationGraph( g2, InformationGraph.GraphType.RADAR, labels, graphDatatoo );

        Information i6 = new InformationGraph( g, InformationGraph.GraphType.DOUGHNUT, labels, graphDatatoo );

        Information i7 = new InformationProgress( g2, "progval", 30 );

        Information i8 = new InformationText( g2, "This is Text!!" );
        Information i9 = new InformationQueryPlan( g2, "THIS IS QUERY PLAN" );

        im.registerInformation( i1, i2, i3, i4, i5, i6, i7, i8, i9 );
    }

}
