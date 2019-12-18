package ch.unibas.dmi.dbis.polyphenydb.statistic.model;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * All the used aggregates
 */
@Slf4j
public enum Aggregate {
    MIN (-1),
    MAX (1);

    @Getter
    private final int equalOperator;

    Aggregate( int i ) {
        this.equalOperator = i;
    }
}
