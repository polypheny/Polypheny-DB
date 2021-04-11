package org.polypheny.db.monitoring;


import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException.SerializationError;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.util.FileSystemManager;


@Slf4j
public class SimpleBackendConnector implements BackendConnector{


    private static final String FILE_PATH = "simpleBackendDb";
    private static DB simpleBackendDb;



    //table name as String mapped to column name of table
    private static BTreeMap<String, String> tableEvents;

    //column_name to distinct entries in column
    private static BTreeMap<String, String> tableColumnEvents;


    //Maybe dynamically added via partition method to make class somewhat exetndable and reusable for other modules
    //ToDO: Think about Register event monitoring?
    //e.g. distinct value of partition column as String to map of epoch and the event
    private static BTreeMap<String, Long> tableValueEvents;


    //Long ID essentially corresponds to EPOCH TIMESTAMP of recorded Time for better traceability
    //from that event get OPERATION = (SELECT|UPDATE|...), DURATION=,...
    private static BTreeMap<Long, MonitorEvent> events;



    public SimpleBackendConnector(){

        initPersistentDB();
    }

    private void initPersistentDB() {


        if ( simpleBackendDb != null ) {
            simpleBackendDb.close();
        }
        synchronized ( this ) {

            File folder = FileSystemManager.getInstance().registerNewFolder( "monitoring" );

            simpleBackendDb = DBMaker.fileDB( new File( folder, this.FILE_PATH ) )
                    .closeOnJvmShutdown()
                    .transactionEnable()
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .make();

            simpleBackendDb.getStore().fileLoad();


            tableEvents = simpleBackendDb.treeMap( "tableEvents", Serializer.STRING, Serializer.STRING ).createOrOpen();
            tableColumnEvents = simpleBackendDb.treeMap( "tableColumnEvents", Serializer.STRING, Serializer.STRING ).createOrOpen();
            tableValueEvents = simpleBackendDb.treeMap( "tableValueEvents", Serializer.STRING, Serializer.LONG ).createOrOpen();
            events = simpleBackendDb.treeMap( "events", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        }

    }

    @Override
    public void initializeConnectorClient() {
        //Nothing really to connect to - Should just reload persisted entries like catalog

        throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
    }


    @Override
    public void monitorEvent() {
        throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
    }


    @Override
    public boolean writeStatisticEvent( long key, MonitorEvent incomingEvent ) {


        log.info( "SimpleBackendConnector received Queue event: " + incomingEvent.monitoringType );
        //throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
        System.out.println("\n");
        synchronized ( this ){
            //events.put(key, incomingEvent);

            log.info( "Write is currently not implemented: See... SimpleBackendConnector.writeStatisticEvent()" );
            simpleBackendDb.commit();
        }
        return true;
    }


    @Override
    public void readStatisticEvent( String outgoingEvent ) {
        throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
    }
}
