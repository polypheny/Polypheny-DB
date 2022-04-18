/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring.workloadAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.util.Pair;

@Slf4j
public class JoinCounter {

    private final Pair<Long, Long> joinInfo;

    @Getter
    @Setter
    private List<Timestamp> timestamps = new ArrayList<>();

    @Getter
    @Setter
    private Timestamp lastTime ;

    @Getter
    @Setter
    private int amount;


    public JoinCounter( Pair<Long, Long> joinInfo, Timestamp timestamp, int amount ) {
        this.joinInfo = joinInfo;
        this.timestamps.add( timestamp );
        this.amount = amount;
    }


    public double getAvgLast() {
        if ( timestamps.size() > 10 ) {
            double timeDifference = 0;
            for ( int i = timestamps.size() - 5; i < timestamps.size(); i++ ) {
                timeDifference += timestamps.get( i ).getTime() - timestamps.get( i - 1 ).getTime();
            }
            return timeDifference / 5;
        } else {
            throw new RuntimeException( "Not possible to get average because there is not enough data yet." );
        }

    }


    public double getAvgComparison() {
        if ( timestamps.size() > 10 ) {
            double timeDifference = 0;
            for ( int i = timestamps.size() - 10; i < timestamps.size() - 5; i++ ) {
                timeDifference += timestamps.get( i ).getTime() -  timestamps.get( i - 1 ).getTime();
            }
            return timeDifference / 5;
        } else {
            throw new RuntimeException( "Not possible to get average because there is not enough data yet." );
        }
    }

}
