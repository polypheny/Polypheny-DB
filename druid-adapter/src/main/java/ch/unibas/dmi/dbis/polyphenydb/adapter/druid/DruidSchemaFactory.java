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

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;

import java.util.List;
import java.util.Map;


/**
 * Schema factory that creates Druid schemas.
 *
 * <table>
 * <caption>Druid schema operands</caption>
 * <tr>
 * <th>Operand</th>
 * <th>Description</th>
 * <th>Required</th>
 * </tr>
 * <tr>
 * <td>url</td>
 * <td>URL of Druid's query node. The default is "http://localhost:8082".</td>
 * <td>No</td>
 * </tr>
 * <tr>
 * <td>coordinatorUrl</td>
 * <td>URL of Druid's coordinator node. The default is <code>url</code>, replacing "8082" with "8081", for example "http://localhost:8081".</td>
 * <td>No</td>
 * </tr>
 * </table>
 */
public class DruidSchemaFactory implements SchemaFactory {

    /**
     * Default Druid URL.
     */
    public static final String DEFAULT_URL = "http://localhost:8082";


    public Schema create( SchemaPlus parentSchema, String name, Map<String, Object> operand ) {
        final String url = operand.get( "url" ) instanceof String
                ? (String) operand.get( "url" )
                : DEFAULT_URL;
        final String coordinatorUrl = operand.get( "coordinatorUrl" ) instanceof String
                ? (String) operand.get( "coordinatorUrl" )
                : url.replace( ":8082", ":8081" );
        // "tables" is a hidden attribute, copied in from the enclosing custom schema
        final boolean containsTables = operand.get( "tables" ) instanceof List && ((List) operand.get( "tables" )).size() > 0;
        return new DruidSchema( url, coordinatorUrl, !containsTables );
    }
}
