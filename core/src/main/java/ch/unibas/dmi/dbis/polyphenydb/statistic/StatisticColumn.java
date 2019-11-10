package ch.unibas.dmi.dbis.polyphenydb.statistic;

import java.util.HashMap;

/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
public class StatisticColumn<T extends Comparable<T>> {
    private final int MAX_MOST_USED_WORDS = 5;
    private final String id;

    private T min;
    private int minAmount;

    private T max;
    private int maxAmount;
    private HashMap<T, Integer> uniqueValues = new HashMap<>();
    private boolean isFull;

    public StatisticColumn(String id, T val) {
        this.id = id;
        put(val);
    }

    /**
     * check for potential "recordable data"
     */
    public void put(T val){
        updateMinMax(val);

        if(!isFull) addUnique(val);
    }

    private void addUnique(T val){
        if(uniqueValues.containsKey(val)){
            uniqueValues.put(val, uniqueValues.get(val) + 1);
        }else if(!isFull && uniqueValues.size() < MAX_MOST_USED_WORDS) {
            uniqueValues.put(val, 1);
            if(uniqueValues.size() > MAX_MOST_USED_WORDS){
                isFull = true;
            }
        }else {
            return;
        }
        //log.info(" updated addUnique " + val.toString());
    }

    private void updateMinMax(T val){
        // just for safety, might delete later...
        if(val instanceof String) {
            return;
        }

        if(this.minAmount == 0){
            this.min = val;
            this.minAmount = 1;
            this.max = val;
            this.maxAmount = 1;
            return;
        }


        int diffMin = val.compareTo(this.min);
        int diffMax = val.compareTo(this.max);


        if(diffMin == 0){
            minAmount++;
        }else if(diffMin < 0){
            this.min = val;
            this.minAmount = 1;
        }

        if(diffMax == 0){
            maxAmount++;
        }else if(diffMax > 0){
            this.max = val;
            this.maxAmount = 1;
        }

        // log.info(" updated min or max " + val.toString());
    }

    public String getId() {
        return id;
    }

    public T getMin() {
        return min;
    }

    public T getMax() {
        return max;
    }

    public HashMap<T, Integer> getUniqueValues() {
        return uniqueValues;
    }
}
