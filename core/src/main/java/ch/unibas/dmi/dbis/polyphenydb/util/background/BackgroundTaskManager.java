package ch.unibas.dmi.dbis.polyphenydb.util.background;


import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskPriority;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskSchedulingType;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class BackgroundTaskManager {

    public static final BackgroundTaskManager INSTANCE = new BackgroundTaskManager();

    private final ConcurrentHashMap<String, BackgroundTaskHandle> tasks = new ConcurrentHashMap<>();

    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    private InformationTable overviewTable;


    private BackgroundTaskManager() {
        informationPage = new InformationPage( "BackgroundTaskManagerPage", "Background Tasks" );
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        overviewTable = new InformationTable(
                informationGroupOverview,
                Arrays.asList( "Class", "Description", " Scheduling Type", "Priority", "Average Time", "Max Time" ) );
        im.registerInformation( overviewTable );

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
            tasks.remove( id );
        } else {
            throw new RuntimeException( "There is no tasks with this id: " + id );
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
                        String.format( Locale.ENGLISH, "%.2f", handle.getAverageExecutionTime() ) + " ms",
                        "" + handle.getMaxExecTime() + " ms" );
            }
        }
    }

}
