package ch.unibas.dmi.dbis.polyphenydb;

import java.util.HashMap;

/**
 * Stores all available statistics  and updates them dynamically
 */
public class StatisticsStore {

    private static StatisticsStore instance = null;

    private HashMap<String, StatisticColumn> store;

    private StatisticsStore(){
        this.store = new HashMap<>();
    }

    public static StatisticsStore StatisticsStore() {
        // To ensure only one instance is created
        if (instance == null) {
            instance = new StatisticsStore();
        }
        return instance;
    }

    public void update(String table, String column, int val){
        if(!this.store.containsKey(table)){
            this.store.put(table, new StatisticColumn(column, val));
        }else {
            this.store.get(table).put(val);
        }
    }
}
