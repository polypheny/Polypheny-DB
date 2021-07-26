package org.polypheny.db.monitoring;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class MonitorEvent implements Serializable {


    private static final long serialVersionUID = 2312903042511293177L;

    public String monitoringType;
    private String description;
    private List<String> fieldNames;
    private Timestamp recordedTimestamp;
    //private RelRoot rel;


}
