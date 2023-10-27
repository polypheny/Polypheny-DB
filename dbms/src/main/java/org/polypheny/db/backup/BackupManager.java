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

package org.polypheny.db.backup;

import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.*;
import org.slf4j.Logger;


public class BackupManager {


    private static BackupManager INSTANCE = null;
    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    //private final Logger logger;


    public BackupManager() {
        informationPage = new InformationPage( "Backup Tasks" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        InformationText startBackup = new InformationText( informationGroupOverview, "Start the Backup." );
        startBackup.setOrder( 1 );
        im.registerInformation( startBackup );

        InformationAction startBackupAction = new InformationAction( informationGroupOverview, "Start", parameters -> {
            //IndexManager.getInstance().resetCounters();
            System.out.println("lol");
            return "Successfully started backup";
        } );
        startBackupAction.setOrder( 2 );
        im.registerInformation( startBackupAction );

        InformationText insertBackupData = new InformationText( informationGroupOverview, "Insert the Backup Data." );
        insertBackupData.setOrder( 3 );
        im.registerInformation( insertBackupData );

        InformationAction insertBackupDataAction = new InformationAction( informationGroupOverview, "Insert", parameters -> {
            //IndexManager.getInstance().resetCounters();
            System.out.println("hii");
            return "Successfully inserted backup data";
        } );

    }

    public static BackupManager setAndGetInstance( BackupManager backupManager ) {
        if ( INSTANCE != null ) {
            throw new GenericRuntimeException( "Setting the BackupInterface, when already set is not permitted." );
        }
        INSTANCE = backupManager;
        return INSTANCE;
    }


}
