package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ArrayBlockingQueue;
import lombok.extern.slf4j.Slf4j;


/**
 * Should serve as a buffer so observed items get put into it
 * and the observer of the ObserverQueue can work on every task sequentially
 */
@Slf4j
public class ObservableQueue extends Observable implements Observer, Runnable {

    private final ArrayBlockingQueue<StatUpdate> queue;
    private final int CAPACITY = 20;


    ObservableQueue() {
        this.queue = new ArrayBlockingQueue<StatUpdate>(CAPACITY);
    }


    public void enqueue( StatUpdate update ) {
        try {
            this.queue.put( update );
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }
    }

    public StatUpdate dequeue(){
        try {
            return this.queue.take();
        } catch ( InterruptedException e ) {
            log.warn( "tried to deque an empty queue" );
        }
        return null;
    }


    @Override
    public void update( Observable o, Object arg ) {

    }


    @Override
    public void run() {

    }


    class StatUpdate {

        private final String key;
        private final String type;


        StatUpdate( String key, String type){
            this.key = key;
            this.type = type;
        }
    }
}
