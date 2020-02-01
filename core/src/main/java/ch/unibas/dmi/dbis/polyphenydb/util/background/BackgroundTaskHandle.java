package ch.unibas.dmi.dbis.polyphenydb.util.background;


import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskPriority;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskSchedulingType;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.commons.lang.time.StopWatch;


class BackgroundTaskHandle implements Runnable {

    @Getter
    private final String id;
    @Getter
    private final BackgroundTask task;
    @Getter
    private final String description;
    @Getter
    private final TaskPriority priority;
    @Getter
    private final TaskSchedulingType schedulingType;

    private final StopWatch stopWatch = new StopWatch();
    private final MovingAverage avgExecTime = new MovingAverage( 100 );
    @Getter
    private long maxExecTime = 0L;

    private ScheduledFuture runner;


    public BackgroundTaskHandle( String id, BackgroundTask task, String description, TaskPriority priority, TaskSchedulingType schedulingType ) {
        this.id = id;
        this.task = task;
        this.description = description;
        this.priority = priority;
        this.schedulingType = schedulingType;


        // Schedule
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        if ( schedulingType == TaskSchedulingType.WORKLOAD ) {
            this.runner = exec.scheduleWithFixedDelay( this, 0, 100, TimeUnit.MILLISECONDS ); // TODO MV: implement workload based scheduling
        } else {
            this.runner = exec.scheduleAtFixedRate( this, 0, schedulingType.getMillis(), TimeUnit.MILLISECONDS );
        }
    }

    public void stop() {
        if(runner != null && !this.runner.isCancelled())
        this.runner.cancel(false);
    }


    public double getAverageExecutionTime() {
        return avgExecTime.getAverage();
    }


    @Override
    public void run() {
        stopWatch.reset();
        stopWatch.start();
        task.backgroundTask();
        stopWatch.stop();
        avgExecTime.add( stopWatch.getTime() );
        if ( maxExecTime < stopWatch.getTime() ) {
            maxExecTime = stopWatch.getTime();
        }
    }


    // https://stackoverflow.com/a/19922501
    private static class MovingAverage {

        private final Queue<Long> window = new LinkedList<Long>();
        private final int period;
        private long sum = 0;


        public MovingAverage( int period ) {
            assert period > 0 : "Period must be a positive integer";
            this.period = period;
        }


        public void add( long x ) {
            sum += x;
            window.add( x );
            if ( window.size() > period ) {
                sum -= window.remove();
            }
        }


        public double getAverage() {
            if ( window.isEmpty() ) {
                return 0.0; // technically the average is undefined
            }
            return sum / (double) window.size();
        }
    }

}
