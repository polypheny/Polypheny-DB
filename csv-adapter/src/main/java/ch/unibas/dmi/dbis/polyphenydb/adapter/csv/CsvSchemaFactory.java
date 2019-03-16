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

package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.model.ModelHandler;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.model.ModelHandler;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;

import java.io.File;
import java.util.Locale;
import java.util.Map;


/**
 * Factory that creates a {@link CsvSchema}.
 *
 * Allows a custom schema to be included in a <code><i>model</i>.json</code> file.
 */
@SuppressWarnings("UnusedDeclaration")
public class CsvSchemaFactory implements SchemaFactory {

    /**
     * Name of the column that is implicitly created in a CSV stream table to hold the data arrival time.
     */
    static final String ROWTIME_COLUMN_NAME = "ROWTIME";

    /**
     * Public singleton, per factory contract.
     */
    public static final CsvSchemaFactory INSTANCE = new CsvSchemaFactory();


    private CsvSchemaFactory() {
    }


    public Schema create( SchemaPlus parentSchema, String name, Map<String, Object> operand ) {
        final String directory = (String) operand.get( "directory" );
        final File base = (File) operand.get( ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName );
        File directoryFile = new File( directory );
        if ( base != null && !directoryFile.isAbsolute() ) {
            directoryFile = new File( base, directory );
        }
        String flavorName = (String) operand.get( "flavor" );
        CsvTable.Flavor flavor;
        if ( flavorName == null ) {
            flavor = CsvTable.Flavor.SCANNABLE;
        } else {
            flavor = CsvTable.Flavor.valueOf( flavorName.toUpperCase( Locale.ROOT ) );
        }
        return new CsvSchema( directoryFile, flavor );
    }
}

