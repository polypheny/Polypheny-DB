/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.polyfier;

import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.information.*;
import org.polypheny.db.polyfier.data.DataUtil;
import org.polypheny.db.polyfier.schemas.DefaultTestEnvironment;
import org.polypheny.db.polyfier.schemas.SchemaUtil;
import org.polypheny.db.util.PolyphenyHomeDirManager;

import java.util.HashMap;
import java.util.List;

public abstract class PolyfierInformation {
    // Constants -----------------------------------------

    private static final String INFO_PAGE_LABEL = "Polyfier";


    public static void configurePolyfierInformation() {

        configureFileSystem();

        InformationManager informationManager = InformationManager.getInstance();;
        HashMap<String, InformationPage> informationPages = new HashMap<>();

        // Information
        InformationGroup group;

        addInformationPage( informationManager, informationPages, "polyfier", "Process", "");
        group = addInformationGroup( informationManager, informationPages, "polyfier", "PolyfierInfo");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Testing Polyfier Functions:" ),
                new InformationAction( group, "TestPolyfierProcess", parameters -> PolyfierProcess.testPolyfierProcess()  )
        ) );


        // Random Schema Generation
        addInformationPage( informationManager, informationPages, "schema", "Schemas", "" );
        group = addInformationGroup( informationManager, informationPages, "schema", "Default");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generating Environments:" ),
                new InformationAction( group, "Default", parameters -> DefaultTestEnvironment.generate()  )
        ) );
        group = addInformationGroup( informationManager, informationPages, "schema", "Random");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generating Environments Randomly:" ),
                new InformationAction( group, "Add DataStores", parameters -> SchemaUtil.addRndSchemaTestDataStores() ),
                new InformationAction( group, "Generate", SchemaUtil::generateSchema )
                        .withParameters( "seed", "refP" )
        ) );

        // Random Data Generation
        addInformationPage( informationManager, informationPages, "data", "Data", "" );
        group = addInformationGroup( informationManager, informationPages, "data", "Actions");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generate Data for Schemas" ),
                new InformationAction( group, "Default", parameters -> generateDefaultData() )
        ) );

    }

    private static void addInformationElements( InformationManager informationManager, InformationGroup group, List<Information> infos ) {
        infos.forEach( group::addInformation );
        infos.forEach( informationManager::registerInformation );
    }

    private static InformationGroup addInformationGroup( InformationManager informationManager, HashMap<String, InformationPage> informationPages, String key, String name ) {
        InformationGroup group = new InformationGroup( informationPages.get( key ), name );
        informationManager.addGroup( group );
        return group;
    }

    private static void addInformationPage( InformationManager informationManager, HashMap<String, InformationPage> informationPages, String key, String title, String description ) {
        informationPages.put( key, new InformationPage( title, description ).setLabel( INFO_PAGE_LABEL ) );
        InformationPage page = informationPages.get( key );
        informationManager.addPage( page );
    }

    private static void configureFileSystem() {
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        fileSystemManager.registerNewFolder( "re-adaptive-optimizer" );
    }

    private static String generateDefaultData() {
        try {
            DataUtil.generateDataForDefaultEnvironment();
        } catch ( UnknownColumnException e ) {
            e.printStackTrace();
            return "Failed";
        }
        return "Success";
    }

}
