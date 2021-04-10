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

    //Long ID essentially corresponds to Atomic ID generated from EventQueue in MonitoringService for better traceability
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


        log.info( "SimpleBackendConnector received Queue event: " + incomingEvent.monitoringType.toString() );
        //throw new RuntimeException("SimpleBackendConnector: Not implemented yet");

        synchronized ( this ){
            //events.put(key, incomingEvent);
            log.info( "Write is ncurrently not implemented: See... SimpleBackendConnector.writeStatisticEvent()" );
            simpleBackendDb.commit();
        }
        return true;
    }


    @Override
    public void readStatisticEvent( String outgoingEvent ) {
        throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
    }
}
