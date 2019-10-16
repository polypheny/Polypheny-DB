/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.function.Function1;


/**
 * Helps to run benchmarks by running the same task repeatedly and averaging the running times.
 *
 * Important: Certain tests are enabled only if logging is enabled at debug level or higher.
 */
@Slf4j
public class Benchmark {

    private final Function1<Statistician, Void> function;
    private final int repeat;
    private final Statistician statistician;


    public Benchmark( String description, Function1<Statistician, Void> function, int repeat ) {
        this.function = function;
        this.repeat = repeat;
        this.statistician = new Statistician( description );
    }


    /**
     * Returns whether performance tests are enabled.
     */
    public static boolean enabled() {
        return log.isDebugEnabled();
    }


    static long printDuration( String desc, long t0 ) {
        final long t1 = System.nanoTime();
        final long duration = t1 - t0;
        log.debug( "{} took {} nanos", desc, duration );
        return duration;
    }


    public void run() {
        for ( int i = 0; i < repeat; i++ ) {
            function.apply( statistician );
        }
        statistician.printDurations();
    }


    /**
     * Collects statistics for a test that is run multiple times.
     */
    public static class Statistician {

        private final String desc;
        private final List<Long> durations = new ArrayList<>();


        public Statistician( String desc ) {
            super();
            this.desc = desc;
        }


        public void record( long start ) {
            durations.add( printDuration( desc + " iteration #" + (durations.size() + 1), start ) );
        }


        private void printDurations() {
            if ( !log.isDebugEnabled() ) {
                return;
            }

            List<Long> coreDurations = durations;
            String durationsString = durations.toString(); // save before sort

            // Ignore the first 3 readings. (JIT compilation takes a while to kick in.)
            if ( coreDurations.size() > 3 ) {
                coreDurations = durations.subList( 3, durations.size() );
            }
            Collections.sort( coreDurations );
            // Further ignore the max and min.
            List<Long> coreCoreDurations = coreDurations;
            if ( coreDurations.size() > 4 ) {
                coreCoreDurations = coreDurations.subList( 1, coreDurations.size() - 1 );
            }
            long sum = 0;
            int count = coreCoreDurations.size();
            for ( long duration : coreCoreDurations ) {
                sum += duration;
            }
            final double avg = ((double) sum) / count;
            double y = 0;
            for ( long duration : coreCoreDurations ) {
                double x = duration - avg;
                y += x * x;
            }
            final double stddev = Math.sqrt( y / count );
            if ( durations.size() == 0 ) {
                log.debug( "{}: {}", desc, "no runs" );
            } else {
                log.debug( "{}: {} first; {} +- {}; {} min; {} max; {} nanos", desc, durations.get( 0 ), avg, stddev, coreDurations.get( 0 ), Util.last( coreDurations ), durationsString );
            }
        }
    }
}

