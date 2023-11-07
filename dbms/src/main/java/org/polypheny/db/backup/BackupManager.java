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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.backup.datagatherer.GatherEntries;
import org.polypheny.db.backup.datagatherer.GatherSchema;
import org.polypheny.db.backup.datainserter.InsertSchema;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.*;


@Slf4j
public class BackupManager {


    private static BackupManager INSTANCE = null;
    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    private BupInformationObject bupInformationObject;
    //private final Logger logger;


    public BackupManager() {
        informationPage = new InformationPage( "Backup Tasks" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        // datagatherer.GatherEntries gatherEntries = new datagatherer.GatherEntries();
        GatherEntries gatherEntries = new GatherEntries();

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        // start backup button
        InformationText startBackup = new InformationText( informationGroupOverview, "Create the Backup." );
        startBackup.setOrder( 1 );
        im.registerInformation( startBackup );

        InformationAction startBackupAction = new InformationAction( informationGroupOverview, "Start", parameters -> {
            //IndexManager.getInstance().resetCounters();
            startDataGathering();
            System.out.println("gather");
            return "Successfully started backup";
        } );
        startBackupAction.setOrder( 2 );
        im.registerInformation( startBackupAction );

        // insert backup-data button
        InformationText insertBackupData = new InformationText( informationGroupOverview, "Insert the Backup Data." );
        insertBackupData.setOrder( 3 );
        im.registerInformation( insertBackupData );

        InformationAction insertBackupDataAction = new InformationAction( informationGroupOverview, "Insert", parameters -> {
            //IndexManager.getInstance().resetCounters();
            startInserting();
            System.out.println("hii");
            return "Successfully inserted backup data";
        } );
        insertBackupDataAction.setOrder( 4 );
        im.registerInformation( insertBackupDataAction );

    }

    public static BackupManager setAndGetInstance( BackupManager backupManager ) {
        if ( INSTANCE != null ) {
            throw new GenericRuntimeException( "Setting the BackupInterface, when already set is not permitted." );
        }
        INSTANCE = backupManager;
        return INSTANCE;
    }

    public void startDataGathering() {
        this.bupInformationObject = new BupInformationObject();
        //GatherEntries gatherEntries = new GatherEntries();
        GatherSchema gatherSchema = new GatherSchema();

        //gatherEntries.start();
        this.bupInformationObject = gatherSchema.start(bupInformationObject);
    }


    private void startInserting() {
        InsertSchema insertSchema = new InsertSchema();

        insertSchema.start(bupInformationObject);
    }

}
