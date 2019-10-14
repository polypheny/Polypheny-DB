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

/**
 * Provides a core set of planner rules.
 *
 * Consider this package to be the "standard library" of planner rules.
 * Most of the common rewrites that you would want to perform on logical relational expressions, or generically on any data source, are present, and have been well tested.
 *
 * Of course, the library is never complete, and contributions are welcome.
 *
 * Not present are rules specific to a particular data source: look in that data source's adapter.
 *
 * Also out of the scope of this package are rules that support a particular operation, such as decorrelation. Those are defined along with the algorithm.
 *
 * For
 *
 * <h2>Related packages and classes</h2>
 * <ul>
 * <li>Package<code> <a href="../../sql/package-summary.html">ch.unibas.dmi.dbis.polyphenydb.sql</a></code> is an object model for SQL expressions</li>
 * <li>Package<code> <a href="../../rex/package-summary.html">ch.unibas.dmi.dbis.polyphenydb.rex</a></code> is an object model for relational row expressions</li>
 * <li>Package<code> <a href="../../plan/package-summary.html">ch.unibas.dmi.dbis.polyphenydb.plan</a></code> provides an optimizer interface.</li>
 * </ul>
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;

