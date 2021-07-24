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

import org.polypheny.db.information.*;

public class UiTestingMonitoringPage {

    private static InformationPage p = new InformationPage( "page1", "MonitoringTestPage", "Test Page 1" );
    private static InformationGroup g = new InformationGroup( p, "Group 1.1" );

    static {
        InformationManager im = InformationManager.getInstance();

        im.addPage( p );
        im.addGroup( g );

        Information i1 = new InformationProgress( g, "progval", 30 );
        Information i2 = new InformationHtml( g, "<b>bold</b>" );

        im.registerInformation( i1, i2 );
    }

}
