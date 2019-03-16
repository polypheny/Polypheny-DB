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

package ch.unibas.dmi.dbis.polyphenydb.model;


import java.util.ArrayList;
import java.util.List;


/**
 * Root schema element.
 *
 * A POJO with fields of {@link Boolean}, {@link String}, {@link ArrayList}, {@link java.util.LinkedHashMap LinkedHashMap}, per Jackson simple data binding.
 *
 * Schema structure is as follows:
 *
 * <!-- CHECKSTYLE: OFF -->
 * <pre>{@code Root}
 *   {@link JsonSchema} (in collection {@link JsonRoot#schemas schemas})
 *     {@link JsonType} (in collection {@link JsonMapSchema#types types})
 *     {@link JsonTable} (in collection {@link JsonMapSchema#tables tables})
 *       {@link JsonColumn} (in collection {@link JsonTable#columns columns})
 *       {@link JsonStream} (in field {@link JsonTable#stream stream})
 *     {@link JsonView}
 *     {@link JsonFunction} (in collection {@link JsonMapSchema#functions functions})
 *     {@link JsonLattice} (in collection {@link JsonSchema#lattices lattices})
 *       {@link JsonMeasure} (in collection {@link JsonLattice#defaultMeasures defaultMeasures})
 *       {@link JsonTile} (in collection {@link JsonLattice#tiles tiles})
 *         {@link JsonMeasure} (in collection {@link JsonTile#measures measures})
 *     {@link JsonMaterialization} (in collection {@link JsonSchema#materializations materializations})
 * </pre>
 * <!-- CHECKSTYLE: ON -->
 *
 * See the <a href="http://calcite.apache.org/docs/model.html">JSON model reference</a>.
 */
public class JsonRoot {

    /**
     * Schema model version number. Required, must have value "1.0".
     */
    public String version;

    /**
     * Name of the schema that will become the default schema for connections to Polypheny-DB that use this model.
     *
     * Optional, case-sensitive. If specified, there must be a schema in this model with this name.
     */
    public String defaultSchema;

    /**
     * List of schema elements.
     *
     * The list may be empty.
     */
    public final List<JsonSchema> schemas = new ArrayList<>();
}

