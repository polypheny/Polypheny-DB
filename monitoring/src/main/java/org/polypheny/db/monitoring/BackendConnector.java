package org.polypheny.db.monitoring;


public interface BackendConnector {

    void initializeConnectorClient();

    void monitorEvent();

    boolean writeStatisticEvent(long key, MonitorEvent incomingEvent);

    void readStatisticEvent(String outgoingEvent);

}
