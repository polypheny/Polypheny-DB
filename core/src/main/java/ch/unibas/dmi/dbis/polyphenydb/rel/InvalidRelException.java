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

package ch.unibas.dmi.dbis.polyphenydb.rel;


/**
 * Exception that indicates that a relational expression would be invalid with given parameters.
 *
 * This exception is thrown by the constructor of a subclass of {@link RelNode} when given parameters it cannot accept. For example, {@code EnumerableJoinRel} can only implement equi-joins,
 * so its constructor throws {@code InvalidRelException} when given the condition {@code input0.x - input1.y = 2}.
 *
 * Because the exception is checked (i.e. extends {@link Exception} but not {@link RuntimeException}), constructors that throw this exception will declare this exception in their {@code throws} clause,
 * and rules that create those relational expressions will need to handle it. Usually a rule will not take the exception personally, and will fail to match. The burden of checking is removed from the rule,
 * which means less code for the author of the rule to maintain.
 *
 * The caller that receives an {@code InvalidRelException} (typically a rule attempting to create a relational expression) should log it at the DEBUG level.
 */
public class InvalidRelException extends Exception {

    /**
     * Creates an InvalidRelException.
     */
    public InvalidRelException( String message ) {
        super( message );
    }


    /**
     * Creates an InvalidRelException with a cause.
     */
    public InvalidRelException( String message, Throwable cause ) {
        super( message, cause );
    }
}
