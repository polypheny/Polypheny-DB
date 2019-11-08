package ch.unibas.dmi.dbis.polyphenydb;

import java.util.HashMap;

/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
public class StatisticColumn {
    private final int MAX_MOST_USED_WORDS = 5;
    private final String id;
    private Integer min = null;
    private Integer max = null;
    private HashMap<String, Integer> mostUsed = new HashMap<>();

    public StatisticColumn(String id, int val) {
        this.id = id;
        put(val);
    }

    public String getId() {
        return id;
    }

    public void put(int val){
        // if no val exist set min & max
        if(this.min == null) {
            this.min = val;
            this.max = val;
        }else {
            if(val < this.min){
                this.min = val;
            }
            if(val > this.max){
                this.max = val;
            }
        }
    }
}
