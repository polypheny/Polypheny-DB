package org.polypheny.db.util.background;


import lombok.Getter;


public interface BackgroundTask {

    void backgroundTask();


    enum TaskPriority {
        LOW, MEDIUM, HIGH
    }


    enum TaskSchedulingType {
        EVERY_SECOND( 1000 ),
        EVERY_FIVE_SECONDS( 5000 ),
        EVERY_TEN_SECONDS( 10000 ),
        EVERY_THIRTY_SECONDS( 30000 ),
        EVERY_MINUTE( 60000 ),
        EVERY_TEN_MINUTES( 600000 ),
        EVERY_FIFTEEN_MINUTES( 900000 ),
        EVERY_THIRTY_MINUTES( 1800000 );

        @Getter
        private long millis;


        TaskSchedulingType( long millis ) {
            this.millis = millis;
        }
    }

}
