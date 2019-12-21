package ch.unibas.dmi.dbis.polyphenydb;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public
class TransactionStat {

    @Getter
    String schema;
    @Getter
    String table;
    @Getter
    String column;


    public TransactionStat( String schema, String table, String column, String data, TransactionStatType type ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.data = data;
        this.type = type;
    }


    public String getTableName() {
        return schema + "." + table;
    }


    public String getColumnName() {
        return getTableName() + "." + column;
    }


    @Getter
    String data;
    @Getter
    TransactionStatType type;
}
