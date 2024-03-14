/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.catalog.logistic;

import org.jetbrains.annotations.NotNull;

public class Pattern {

    public final String pattern;
    public final boolean containsWildcards;


    public Pattern( String pattern ) {
        this.pattern = pattern;
        containsWildcards = pattern.contains( "%" ) || pattern.contains( "_" );
    }


    public Pattern toLowerCase() {
        return new Pattern( pattern.toLowerCase() );
    }


    public static Pattern of( @NotNull String pattern ) {
        return new Pattern( pattern );
    }


    public String toRegex() {
        return pattern.replace( "_", "(.)" ).replace( "%", "(.*)" );
    }


    @Override
    public String toString() {
        return "Pattern[" + pattern + "]";
    }

}
