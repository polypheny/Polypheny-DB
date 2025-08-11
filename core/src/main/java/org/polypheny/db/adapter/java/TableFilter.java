/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.java;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableFilter {

    Oracle(
            Set.of( "AQ$_", "LOG", "MVIEW$_", "OL$", "REDO_", "REPL_", "ROLLING$", "SCHEDULER", "SQLPLUS", "HELP" )
    ),

    MySQL(
            Set.of( "sys_config" )
    ),

    GENERIC( Set.of() );

    public final Set<String> ignoredTables;


    TableFilter( final Set<String> ignoredTables ) {
        this.ignoredTables = ignoredTables.stream()
                .map( String::trim )
                .collect( Collectors.toSet() );
    }


    public static TableFilter forAdapter( String adapterName ) {
        return Arrays.stream( values() )
                .filter( f -> f.name().equalsIgnoreCase( adapterName ) )
                .findFirst()
                .orElse( GENERIC );
    }


    public boolean shouldIgnore( String tableName ) {
        String upper = tableName.toUpperCase();
        return ignoredTables.stream()
                .map( String::toUpperCase )
                .anyMatch( upper::startsWith );
    }


}
