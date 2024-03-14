package org.polypheny.db.util.background;


import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;


public class BackgroundTaskManager {

    public static final BackgroundTaskManager INSTANCE = new BackgroundTaskManager();

    private final ConcurrentHashMap<String, BackgroundTaskHandle> tasks = new ConcurrentHashMap<>();

    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    private InformationTable overviewTable;


    private BackgroundTaskManager() {
        informationPage = new InformationPage( "Background Tasks" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        overviewTable = new InformationTable(
                informationGroupOverview,
                Arrays.asList( "Class", "Description", " Scheduling Type", "Priority", "Average Time", "Max Time" ) );
        im.registerInformation( overviewTable.fullWidth( true ) );

        BackgroundTaskInfo backgroundTaskInfo = new BackgroundTaskInfo();
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate( backgroundTaskInfo, 0, 5, TimeUnit.SECONDS );
    }


    public String registerTask( BackgroundTask task, String description, TaskPriority priority, TaskSchedulingType schedulingType ) {
        String id = UUID.randomUUID().toString();
        tasks.put( id, new BackgroundTaskHandle( id, task, description, priority, schedulingType ) );
        return id;
    }


    public void removeBackgroundTask( String id ) {
        if ( tasks.containsKey( id ) ) {
            tasks.get( id ).stop();
            tasks.remove( id );
        } else {
            throw new GenericRuntimeException( "There is no tasks with this id: " + id );
        }
    }


    private class BackgroundTaskInfo implements Runnable {

        @Override
        public void run() {
            overviewTable.reset();
            for ( BackgroundTaskHandle handle : tasks.values() ) {
                overviewTable.addRow(
                        handle.getTask().getClass().getSimpleName(),
                        handle.getDescription(),
                        handle.getSchedulingType().name(),
                        handle.getPriority().name(),
                        String.format( Locale.ENGLISH, "%.2f", handle.getAverageExecutionTime() ) + " ms", handle.getMaxExecTime() + " ms" );
            }
        }

    }

}
