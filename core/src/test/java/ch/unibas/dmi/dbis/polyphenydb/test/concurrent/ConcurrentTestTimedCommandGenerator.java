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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test.concurrent;


import com.google.common.collect.ImmutableList;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * ConcurrentTestTimedCommandGenerator extends {@link ConcurrentTestCommandGenerator} and repeats the configured command sequence until a certain amount of time has elapsed.
 *
 * The command sequence is always completed in full, even if the time limit has been exceeded. Therefore, the time limit can only be considered the minimum length of time that the test will run and not a guarantee of how long the test will take.
 */
public class ConcurrentTestTimedCommandGenerator extends ConcurrentTestCommandGenerator {

    private int runTimeSeconds;
    private long endTimeMillis;


    /**
     * Constructs a new ConcurrentTestTimedCommandGenerator that will run for at least the given amount of time. See {@link ConcurrentTestTimedCommandGenerator} for more information on the semantics of run-time length.
     *
     * @param runTimeSeconds minimum run-time length, in seconds
     */
    public ConcurrentTestTimedCommandGenerator( int runTimeSeconds ) {
        super();
        this.runTimeSeconds = runTimeSeconds;
    }


    /**
     * Retrieves an Iterator based on the configured commands. This Iterator, when it reaches the end of the command list will compare the current time with the test's end time. If there is time left, the Iterator will repeat the command sequence.
     *
     * The test's end time is computed by taking the value of <code>System.currentTimeMillis()</code> the first time this method is called (across all thread IDs) and adding the configured run time.
     *
     * @param threadId the thread ID to get an Iterator on
     */
    Iterable<ConcurrentTestCommand> getCommandIterable( final int threadId ) {
        synchronized ( this ) {
            if ( endTimeMillis == 0L ) {
                endTimeMillis = System.currentTimeMillis() + (runTimeSeconds * 1000);
            }
        }

        return () -> new TimedIterator<ConcurrentTestCommand>( getCommands( threadId ), endTimeMillis );
    }


    /**
     * Outputs command sequence and notes how long the sequence will be repeated.
     */
    void printCommands( PrintStream out, Integer threadId ) {
        super.printCommands( out, threadId );
        out.println( "Repeat sequence for " + runTimeSeconds + " seconds" );
    }


    /**
     * TimedIterator is an Iterator that repeats a given collection's elements until <code>System.currentTimeMillis() &ge; endTimeMillis</code>.
     *
     * @param <E> element type
     */
    private class TimedIterator<E> implements Iterator<E> {

        private final List<E> commands;
        private long endTimeMillis;
        private int commandIndex;


        private TimedIterator( Collection<E> commands, long endTimeMillis ) {
            this.commands = ImmutableList.copyOf( commands );
            this.endTimeMillis = endTimeMillis;
            this.commandIndex = 0;
        }


        public boolean hasNext() {
            if ( commandIndex < commands.size() ) {
                return true;
            }

            if ( System.currentTimeMillis() < endTimeMillis ) {
                commandIndex = 0;
                return commands.size() > 0; // handle empty array
            }

            return false;
        }


        public E next() {
            if ( !hasNext() ) {
                throw new NoSuchElementException();
            }

            return commands.get( commandIndex++ );
        }


        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

