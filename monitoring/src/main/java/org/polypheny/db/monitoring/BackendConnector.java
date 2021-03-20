package org.polypheny.db.monitoring;


public interface BackendConnector {

    void initializeConnectorClient();

    void monitorEvent();

    void writeStatisticEvent(String incomingEvent);

    void readStatisticEvent(String outgoingEvent);

}
