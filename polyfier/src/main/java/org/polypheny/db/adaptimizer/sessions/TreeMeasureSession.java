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

package org.polypheny.db.adaptimizer.sessions;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.adaptimizer.ReAdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.rndqueries.AbstractQuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QuerySupplier;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class TreeMeasureSession extends QuerySession {

    public TreeMeasureSession( AbstractQuerySupplier querySupplier ) {
        super( querySupplier );
    }

    @Override
    public void run() {
        log.info( "T,Iter,Faults,Used Memory %" );
        StopWatch stopWatch = new StopWatch();

        for ( int i = 1; i < 1000; i += 1 ) {
            stopWatch.reset();
            stopWatch.start();
            for ( int  j = 1; j < 10000; j++ ) {
                Triple<Statement, AlgNode, Long> execTriple =  getQuerySupplier().get();
                ReAdaptiveOptimizerImpl.getTransactionManager().removeTransaction( execTriple.getLeft().getTransaction().getXid() );
            }
            stopWatch.stop();
            log.debug( "{},{},{},{}", stopWatch.getTime(), i, ((QuerySupplier)getQuerySupplier()).earlyFaults, (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/(double)Runtime.getRuntime().totalMemory() );
            ((QuerySupplier)getQuerySupplier()).earlyFaults = 0;
        }
    }

}
