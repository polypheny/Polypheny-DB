package ch.unibas.dmi.dbis.polyphenydb.statistic;

import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.*;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * Stores all available statistics  and updates them dynamically
 */
public class StatisticsStore {

    private static StatisticsStore instance = null;

    private TransactionManager transactionManager;

    // TODO private again
    public HashMap<String, StatisticColumn> store;

    private String databaseName = "APP";
    private String userName = "pa";

    private static final Logger LOGGER = LoggerFactory.getLogger( StatisticsStore.class );

    private StatisticsStore(){
        this.store = new HashMap<>();
    }

    public static StatisticsStore getInstance() {
        // To ensure only one instance is created
        if (instance == null) {
            instance = new StatisticsStore();
        }
        return instance;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void update(String table, String column, int val){
        if(!this.store.containsKey(table)){
            this.store.put(table, new StatisticColumn(column, val));
        }else {
            this.store.get(table).put(val);
        }
    }

    public void updateAll(String table, String column, stringList vals) {
        vals.forEach(val -> {
            // still not sure if generic or not
            update(table, column, val);
        });
    }

    public void updateAll(String table, String column, numericalList vals) {
        vals.forEach(val -> {
            // still not sure if generic or not
            update(table, column, val);
        });
    }

    public void update(String table, String column, String val){
        if(!this.store.containsKey(table)){
            this.store.put(table, new StatisticColumn(column, val));
        }else {
            this.store.get(table).put(val);
        }
    }

    /**
     * Reset all statistics and reevaluate them
     */
    public void reevaluateStore(){
        this.store.clear();

        Transaction transaction = getTransaction();
        try {
            CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( databaseName );
            CatalogCombinedDatabase combinedDatabase = transaction.getCatalog().getCombinedDatabase( catalogDatabase.id );
        } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException | GenericCatalogException e ) {
            LOGGER.error( "Caught exception", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                LOGGER.error( "Caught exception while rollback", e );
            }
        }

    }


    public HashMap<String, StatisticColumn> getStore(){
        return this.store;
    }

    private Transaction getTransaction() {
        return getTransaction( false );
    }


    private Transaction getTransaction( boolean analyze ) {
        try {
            return transactionManager.startTransaction( userName, databaseName, analyze );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

    interface numericalList extends List<Integer>{}

    interface stringList extends List<String>{};

}
