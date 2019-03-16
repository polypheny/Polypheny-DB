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


import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.util.Source;
import ch.unibas.dmi.dbis.polyphenydb.util.Sources;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.Map;


/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {

    private final File directoryFile;
    private final CsvTable.Flavor flavor;
    private Map<String, Table> tableMap;


    /**
     * Creates a CSV schema.
     *
     * @param directoryFile Directory that holds {@code .csv} files
     * @param flavor Whether to instantiate flavor tables that undergo query optimization
     */
    public CsvSchema( File directoryFile, CsvTable.Flavor flavor ) {
        super();
        this.directoryFile = directoryFile;
        this.flavor = flavor;
    }


    /**
     * Looks for a suffix on a string and returns either the string with the suffix removed or the original string.
     */
    private static String trim( String s, String suffix ) {
        String trimmed = trimOrNull( s, suffix );
        return trimmed != null ? trimmed : s;
    }


    /**
     * Looks for a suffix on a string and returns either the string with the suffix removed or null.
     */
    private static String trimOrNull( String s, String suffix ) {
        return s.endsWith( suffix )
                ? s.substring( 0, s.length() - suffix.length() )
                : null;
    }


    @Override
    protected Map<String, Table> getTableMap() {
        if ( tableMap == null ) {
            tableMap = createTableMap();
        }
        return tableMap;
    }


    private Map<String, Table> createTableMap() {
        // Look for files in the directory ending in ".csv", ".csv.gz", ".json", ".json.gz".
        final Source baseSource = Sources.of( directoryFile );
        File[] files = directoryFile.listFiles( ( dir, name ) -> {
            final String nameSansGz = trim( name, ".gz" );
            return nameSansGz.endsWith( ".csv" ) || nameSansGz.endsWith( ".json" );
        } );
        if ( files == null ) {
            System.out.println( "directory " + directoryFile + " not found" );
            files = new File[0];
        }
        // Build a map from table name to table; each file becomes a table.
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for ( File file : files ) {
            Source source = Sources.of( file );
            Source sourceSansGz = source.trim( ".gz" );
            final Source sourceSansJson = sourceSansGz.trimOrNull( ".json" );
            if ( sourceSansJson != null ) {
                JsonTable table = new JsonTable( source );
                builder.put( sourceSansJson.relative( baseSource ).path(), table );
                continue;
            }
            final Source sourceSansCsv = sourceSansGz.trim( ".csv" );

            final Table table = createTable( source );
            builder.put( sourceSansCsv.relative( baseSource ).path(), table );
        }
        return builder.build();
    }


    /**
     * Creates different sub-type of table based on the "flavor" attribute.
     */
    private Table createTable( Source source ) {
        switch ( flavor ) {
            case TRANSLATABLE:
                return new CsvTranslatableTable( source, null );
            case SCANNABLE:
                return new CsvScannableTable( source, null );
            case FILTERABLE:
                return new CsvFilterableTable( source, null );
            default:
                throw new AssertionError( "Unknown flavor " + this.flavor );
        }
    }
}

