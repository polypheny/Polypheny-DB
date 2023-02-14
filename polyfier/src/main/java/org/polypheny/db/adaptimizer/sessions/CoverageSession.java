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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.adaptimizer.ReAdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.rndqueries.AbstractQuerySupplier;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class CoverageSession extends QuerySession {
    TransactionManager transactionManager;
    PolyphenyHomeDirManager polyphenyHomeDirManager;

    public CoverageSession( AbstractQuerySupplier querySupplier ) {
        super( querySupplier );
        transactionManager = ReAdaptiveOptimizerImpl.getTransactionManager();
        this.polyphenyHomeDirManager = PolyphenyHomeDirManager.getInstance();
        polyphenyHomeDirManager.registerNewFolder( "measurements" );
        polyphenyHomeDirManager.registerNewFile( "measurements/coverage.txt" );
    }


    @Override
    public void run() {
        File file = polyphenyHomeDirManager.getFileIfExists( "measurements/coverage.txt" );

        BufferedWriter writer;
        try {
            writer = new BufferedWriter( new FileWriter( file ) );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        int factor = 10;
        int iter = 100000;
        int count = 0;

        //BitSet bitSet = new BitSet();
        HashSet<Integer> set = new HashSet<>();
        StopWatch stopWatch = new StopWatch();

        for ( int i = 0; i < factor; i++ ) {
            stopWatch.reset();
            stopWatch.start();
            for ( int j = 0; j < iter; j++ ) {
                Triple<Statement, AlgNode, Long> execTriple = getQuerySupplier().get();
                // transactionManager.removeTransaction( execTriple.getLeft().getTransaction().getXid() );
                int hash = AlgOptUtil.toString( execTriple.getMiddle(), ExplainLevel.NO_ATTRIBUTES ).hashCode();
                set.add( hash );
                count++;
                if ( j % 1000 == 0 ) {
                    log.debug( "{}, {}, {}%", count, set.size(), ( set.size() / (double) count )*100 );
                }
            }
            stopWatch.stop();
            long t = TimeUnit.MILLISECONDS.convert( stopWatch.getTime(), TimeUnit.MINUTES ) / iter;

            try {
                int card = set.size();
                log.debug( "Uniques: {}, Time/Q {}, Ratio: {}%", card, t, (card/(double)(i * iter))*100 );
                writer.write( i + "," + count + "," + card + "," + t );
                writer.newLine();
                writer.flush();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }

        try {

            writer.close();

        } catch ( IOException e ) {

            throw new RuntimeException( e );

        }
    }

}
