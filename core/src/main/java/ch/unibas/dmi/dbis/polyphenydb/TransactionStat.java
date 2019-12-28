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
    @Getter
    String data;
    @Getter
    TransactionStatType transactionType;


    public TransactionStat( String schema, String table, String column, String data, TransactionStatType transactionType ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.data = data;
        this.transactionType = transactionType;
    }


    public TransactionStat( String schema, String table, String column, TransactionStatType transactionType ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.transactionType = transactionType;
    }


    public String getTableName() {
        return schema + "." + table;
    }


    public String getColumnName() {
        return getTableName() + "." + column;
    }

}
