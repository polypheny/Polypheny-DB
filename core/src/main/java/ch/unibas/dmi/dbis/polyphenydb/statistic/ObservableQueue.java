package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.Observable;
import java.util.Observer;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Queue;


/**
 * Should serve as a buffer so observed items get put into it
 * and the observer of the ObserverQueue can work on every task sequentially
 */
@Slf4j
public class ObservableQueue extends Observable implements Observer, Runnable {

    private final Queue<StatUpdate> queue;


    ObservableQueue() {
        this.queue = new Queue<StatUpdate>();
    }


    public void enqueue( StatUpdate update ) {
        this.queue.enqueue( update );
    }

    public StatUpdate dequeue(){
        try {
            return this.queue.dequeue();
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
