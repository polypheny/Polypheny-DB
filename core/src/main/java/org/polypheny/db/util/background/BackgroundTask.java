package org.polypheny.db.util.background;


import lombok.Getter;


public interface BackgroundTask {

    void backgroundTask();


    enum TaskPriority {
        LOW, MEDIUM, HIGH
    }


    @Getter
    enum TaskSchedulingType {
        EVERY_SECOND( 1000, TaskDelayType.DELAYED ),
        EVERY_FIVE_SECONDS( 5000, TaskDelayType.DELAYED ),
        EVERY_TEN_SECONDS( 10000, TaskDelayType.DELAYED ),
        EVERY_THIRTY_SECONDS( 30000, TaskDelayType.DELAYED ),
        EVERY_MINUTE( 60000, TaskDelayType.DELAYED ),
        EVERY_TEN_MINUTES( 600000, TaskDelayType.DELAYED ),
        EVERY_FIFTEEN_MINUTES( 900000, TaskDelayType.DELAYED ),
        EVERY_THIRTY_MINUTES( 1800000, TaskDelayType.DELAYED ),

        EVERY_SECOND_FIXED( 1000, TaskDelayType.FIXED ),
        EVERY_FIVE_SECONDS_FIXED( 5000, TaskDelayType.FIXED ),
        EVERY_TEN_SECONDS_FIXED( 10000, TaskDelayType.FIXED ),
        EVERY_THIRTY_SECONDS_FIXED( 30000, TaskDelayType.FIXED ),
        EVERY_MINUTE_FIXED( 60000, TaskDelayType.FIXED ),
        EVERY_TEN_MINUTES_FIXED( 600000, TaskDelayType.FIXED ),
        EVERY_FIFTEEN_MINUTES_FIXED( 900000, TaskDelayType.FIXED ),
        EVERY_THIRTY_MINUTES_FIXED( 1800000, TaskDelayType.FIXED );

        private final TaskDelayType delayType;
        private final long millis;


        TaskSchedulingType( long millis, TaskDelayType delayType ) {
            this.millis = millis;
            this.delayType = delayType;
        }

    }


    enum TaskDelayType {
        FIXED, DELAYED
    }

}
