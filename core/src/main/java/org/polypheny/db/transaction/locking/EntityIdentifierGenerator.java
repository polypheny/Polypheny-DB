package org.polypheny.db.transaction.locking;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.NotImplementedException;

public class EntityIdentifierGenerator {

    private static final int QUEUE_SIZE = 10000;
    private static final int MAX_IDENTIFIER = 100000;

    public static final EntityIdentifierGenerator INSTANCE = new EntityIdentifierGenerator();
    private static final String GENERATOR_LOG = "entry_identifier_counter.dat";

    private final AtomicLong identifierCounter;
    private final BlockingQueue<Long> identifierQueue = new LinkedBlockingQueue<>( QUEUE_SIZE );
    private boolean scanForIds = false;


    private EntityIdentifierGenerator() {
        long initialCounterValue = loadState();
        this.identifierCounter = new AtomicLong( initialCounterValue );
        fillQueue();
    }


    public long getEntryIdentifier() {
        try {
            long entryIdentifier = identifierQueue.take();
            if ( identifierQueue.isEmpty() ) {
                fillQueue();
            }
            return entryIdentifier;
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Interrupted while fetching an entry identifier", e );
        }
    }


    private void fillQueue() {
        if ( scanForIds ) {
            fillQueueFromScan();
            return;
        }
        fillQueueFromCounter();
    }


    private void fillQueueFromCounter() {
        try {
            while ( identifierQueue.remainingCapacity() > 0 && identifierCounter.get() < MAX_IDENTIFIER ) {
                identifierQueue.put( identifierCounter.incrementAndGet() );
            }
            if ( identifierCounter.get() >= MAX_IDENTIFIER ) {
                scanForIds = true;
            }
            if ( identifierQueue.remainingCapacity() > 0 ) {
                fillQueueFromScan();
            }
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Error filling the queue", e );
        }
    }


    private void fillQueueFromScan() {
        try {
            List<Long> unassignedIdentifiers = findUnassignedIdentifiers( QUEUE_SIZE );
            if ( unassignedIdentifiers.isEmpty() ) {
                throw new RuntimeException( "No more unassigned identifiers available" );
            }
            for ( Long identifier : unassignedIdentifiers ) {
                if (identifierQueue.remainingCapacity() == 0 ) {
                    break;
                }
                identifierQueue.put( identifier );
            }
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Error filling the queue in Mode B", e );
        }
    }


    private List<Long> findUnassignedIdentifiers( int count ) {
        // ToDo TH: Run a scan on the database to find unassigned identifiers
        throw new NotImplementedException( "Unassigned Entry Id Scanning not implemented yet" );
    }


    public void shutdown() {
        try ( DataOutputStream dataOutputStream = new DataOutputStream( new FileOutputStream( GENERATOR_LOG ) ) ) {
            dataOutputStream.writeLong( identifierCounter.get() );
            dataOutputStream.writeBoolean( scanForIds );
        } catch ( IOException e ) {
            System.err.println( "Error saving state: " + e.getMessage() );
        }
    }


    private long loadState() {
        File file = new File( GENERATOR_LOG );
        if ( !file.exists() ) {
            this.scanForIds = false;
            return 0;
        }

        try ( DataInputStream DataInputStream = new DataInputStream( new FileInputStream( file ) ) ) {
            long counterValue = DataInputStream.readLong();
            this.scanForIds = DataInputStream.readBoolean();
            return counterValue;
        } catch ( IOException e ) {
            System.err.println( "Error loading state, starting from 0: " + e.getMessage() );
            scanForIds = false;
            return 0;
        }
    }

}
