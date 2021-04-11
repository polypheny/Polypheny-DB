package org.polypheny.db.monitoring;


import java.io.Serializable;
import java.security.Signature;
import java.sql.Timestamp;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;


@Getter
@Builder
public class MonitorEvent implements Serializable {


    private static final long serialVersionUID = 2312903042511293177L;

    public String monitoringType;
    private String description;
    private List<String> fieldNames;
    private long recordedTimestamp;
    private RelRoot routed;
    private PolyphenyDbSignature signature;
    private Statement statement;
    private List<List<Object>> rows;
    @Setter
    private RelOptTable table;


}
