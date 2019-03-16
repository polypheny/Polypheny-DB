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


/**
 * ConcurrentTestCommand represents a command, sequentially executed by {@link ConcurrentTestCommandExecutor}, during a concurrency test
 *
 * ConcurrentTestCommand instances are normally instantiated by the {@link ConcurrentTestCommandGenerator} class.
 */
public interface ConcurrentTestCommand {

    /**
     * Executes this command. The ConcurrentTestCommandExecutor provides access to a JDBC connection and previously prepared statements.
     *
     * @param exec the ConcurrentTestCommandExecutor firing this command.
     * @throws Exception to indicate a test failure
     * @see ConcurrentTestCommandExecutor#getStatement()
     * @see ConcurrentTestCommandExecutor#setStatement(java.sql.Statement)
     */
    void execute( ConcurrentTestCommandExecutor exec ) throws Exception;

    /**
     * Marks a command to show that it is expected to fail, and indicates how. Used for negative tests. Normally when a command fails the embracing test fails.
     * But when a marked command fails, the error is caught and inspected: if it matches the expected error, the test continues. However if it does not match, if another kind of exception is thrown, or if no exception is caught, then the test fails.
     * Assumes the error is indicated by a java.sql.SQLException. Optionally checks for the expected error condition by matching the error message against a regular expression. (Scans the list of chained SQLExceptions).
     *
     * @param comment a brief description of the expected error
     * @param pattern null, or a regular expression that matches the expected
     * error message.
     */
    ConcurrentTestCommand markToFail(
            String comment,
            String pattern );

    /**
     * Returns true if the command should fail. This allows special error handling for expected failures that don't have patterns.
     *
     * @return true if command is expected to fail
     */
    boolean isFailureExpected();

    /**
     * Set this command to expect a patternless failure.
     */
    ConcurrentTestCommand markToFail();


    /**
     * Indicates that a command should have failed, but instead succeeded, which is a test error
     */
    class ShouldHaveFailedException extends RuntimeException {

        private final String description;


        public ShouldHaveFailedException( String description ) {
            this.description = description;
        }


        public String getDescription() {
            return description;
        }
    }
}

