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

import org.polypheny.db.information.*;
//import org.polypheny.db.util.background.BackgroundTaskHandle;


public class BackupInterface {


    public static final BackupInterface INSTANCE = new BackupInterface();

    //private final ConcurrentHashMap<String, BackgroundTaskHandle> tasks = new ConcurrentHashMap<>();
    public static BackupInterface init() {
        int lol = 1;
        return INSTANCE;
    }
    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    //private InformationTable overviewTable;


    private BackupInterface() {
        informationPage = new InformationPage( "Backup Tasks" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        /*
        overviewTable = new InformationTable(
                informationGroupOverview,
                Arrays.asList( "Class", "Description", " Scheduling Type", "Priority", "Average Time", "Max Time" ) );
        im.registerInformation( overviewTable );

        BackupInterface.BackgroundTaskInfo backgroundTaskInfo = new BackgroundTaskInfo();
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate( backgroundTaskInfo, 0, 5, TimeUnit.SECONDS );

         */
    }

    /*
    public String registerTask( BackupInterface task, String description, BackupInterface.TaskPriority priority, BackupInterface.TaskSchedulingType schedulingType ) {
        String id = UUID.randomUUID().toString();
        tasks.put( id, new BackgroundTaskHandle( id, task, description, priority, schedulingType ) );
        return id;
    }

     */

    /*
    public void removeBackgroundTask( String id ) {
        if ( tasks.containsKey( id ) ) {
            tasks.get( id ).stop();
            tasks.remove( id );
        } else {
            throw new RuntimeException( "There is no tasks with this id: " + id );
        }
    }

     */


    private class BackgroundTaskInfo implements Runnable {

        @Override
        public void run() {
            //overviewTable.reset();
            /*
            for ( BackgroundTaskHandle handle : tasks.values() ) {
                overviewTable.addRow(
                        handle.getTask().getClass().getSimpleName(),
                        handle.getDescription(),
                        handle.getSchedulingType().name(),
                        handle.getPriority().name(),
                        String.format( Locale.ENGLISH, "%.2f", handle.getAverageExecutionTime() ) + " ms", "" + handle.getMaxExecTime() + " ms" );
            }

             */
        }
    }

}
