package org.polypheny.db.monitoring;


import java.sql.Timestamp;
import java.util.List;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class MonitorEvent {

    public String monitoringType;
    private String description;
    private List<String> fieldNames;
    private Timestamp recordedTimestamp;


}
