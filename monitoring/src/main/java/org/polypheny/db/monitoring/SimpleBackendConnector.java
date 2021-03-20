package org.polypheny.db.monitoring;


public class SimpleBackendConnector implements BackendConnector{

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
    public void writeStatisticEvent( String incomingEvent ) {
        throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
    }


    @Override
    public void readStatisticEvent( String outgoingEvent ) {
        throw new RuntimeException("SimpleBackendConnector: Not implemented yet");
    }
}
