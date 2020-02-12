package ch.unibas.dmi.dbis.polyphenydb.util.background;


import lombok.Getter;


public interface BackgroundTask {

    void backgroundTask();


    enum TaskPriority {
        LOW, MEDIUM, HIGH
    }


    enum TaskSchedulingType {
        WORKLOAD( 0 ),
        EVERY_SECOND( 1000 ),
        EVERY_FIVE_SECONDS( 5000 ),
        EVERY_TEN_SECONDS( 10000 ),
        EVERY_THIRTY_SECONDS( 30000 ),
        EVERY_MINUTE( 60000 );

        @Getter
        private long millis;


        TaskSchedulingType( long millis ) {
            this.millis = millis;
        }
    }

}
