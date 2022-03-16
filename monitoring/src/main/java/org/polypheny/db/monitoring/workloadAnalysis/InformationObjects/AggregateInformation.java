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

package org.polypheny.db.monitoring.workloadAnalysis.InformationObjects;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;

@Slf4j
@Getter
public class AggregateInformation {

    private Kind kind;
    private int minCount = 0;
    private int maxCount = 0;
    private int sumCount = 0;
    private int avgCount = 0;
    private int countCount = 0;
    private int overAllCount;

    public AggregateInformation( ) {
        this.overAllCount = maxCount + maxCount + sumCount + avgCount + countCount;
    }

    public AggregateInformation(
            int minCount,
            int maxCount,
            int sumCount,
            int avgCount,
            int countCount ) {
        this.minCount = minCount;
        this.maxCount = maxCount;
        this.sumCount = sumCount;
        this.avgCount = avgCount;
        this.countCount = countCount;
        this.overAllCount = maxCount + maxCount + sumCount + avgCount + countCount;
    }


    public void incrementAggregateInformation( Kind kind ) {
        this.overAllCount += 1;
        switch ( kind ){
            case MIN:
                this.minCount += 1;
                break;
            case MAX:
                this.maxCount += 1;
                break;
            case SUM:
                this.sumCount += 1;
                break;
            case AVG:
                this.avgCount += 1;
                break;
            case COUNT:
                this.countCount += 1;
                break;
            default:
                log.warn("This kind of Aggregation is not implemented yet. (AggregateInformation)" + kind);
                //throw new RuntimeException("This kind of Aggregation is not implemented yet.");
        }
    }

    public void updateAggregateInformation(AggregateInformation aggregateInformation){
        this.minCount = this.minCount + aggregateInformation.getMinCount();
        this.maxCount = this.maxCount + aggregateInformation.getMaxCount();
        this.sumCount = this.sumCount + aggregateInformation.getSumCount();
        this.avgCount = this.avgCount + aggregateInformation.getAvgCount();
        this.countCount = this.countCount + aggregateInformation.getCountCount();
        this.overAllCount = maxCount + maxCount + sumCount + avgCount + countCount;
    }

}
