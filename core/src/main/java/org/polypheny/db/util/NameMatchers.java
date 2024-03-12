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

package org.polypheny.db.util;


import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;


/**
 * Helpers for {@link NameMatcher}.
 */
public class NameMatchers {

    private static final BaseMatcher CASE_SENSITIVE = new BaseMatcher( true );
    private static final BaseMatcher CASE_INSENSITIVE = new BaseMatcher( false );


    private NameMatchers() {
    }


    /**
     * Returns a name matcher with the given case sensitivity.
     */
    public static NameMatcher withCaseSensitive( final boolean caseSensitive ) {
        return caseSensitive ? CASE_SENSITIVE : CASE_INSENSITIVE;
    }


    /**
     * Creates a name matcher that can suggest corrections to what the user typed. It matches liberally (case-insensitively) and also records the last match.
     */
    public static NameMatcher liberal() {
        return new LiberalNameMatcher();
    }


    /**
     * Partial implementation of {@link NameMatcher}.
     */
    private static class BaseMatcher implements NameMatcher, Serializable {

        private final boolean caseSensitive;


        BaseMatcher( boolean caseSensitive ) {
            this.caseSensitive = caseSensitive;
        }


        @Override
        public boolean isCaseSensitive() {
            return caseSensitive;
        }


        @Override
        public boolean matches( String string, String name ) {
            return caseSensitive
                    ? string.equals( name )
                    : string.equalsIgnoreCase( name );
        }


        protected boolean listMatches( List<String> list0, List<String> list1 ) {
            if ( list0.size() != list1.size() ) {
                return false;
            }
            for ( int i = 0; i < list0.size(); i++ ) {
                String s0 = list0.get( i );
                String s1 = list1.get( i );
                if ( !matches( s0, s1 ) ) {
                    return false;
                }
            }
            return true;
        }


        @Override
        public <K extends List<String>, V> V get( Map<K, V> map, List<String> prefixNames, List<String> names ) {
            final List<String> key = concat( prefixNames, names );
            if ( caseSensitive ) {
                //noinspection SuspiciousMethodCalls
                return map.get( key );
            }
            for ( Map.Entry<K, V> entry : map.entrySet() ) {
                if ( listMatches( key, entry.getKey() ) ) {
                    matched( prefixNames, entry.getKey() );
                    return entry.getValue();
                }
            }
            return null;
        }


        private List<String> concat( List<String> prefixNames, List<String> names ) {
            if ( prefixNames.isEmpty() ) {
                return names;
            } else {
                return ImmutableList.<String>builder()
                        .addAll( prefixNames )
                        .addAll( names )
                        .build();
            }
        }


        protected void matched( List<String> prefixNames, List<String> names ) {
        }


        protected List<String> bestMatch() {
            throw new UnsupportedOperationException();
        }


        @Override
        public String bestString() {
            return Util.sepList( toStar( bestMatch() ), "." );
        }


        public List<String> toStar( List<String> names ) {
            return names.stream().map( s -> s.isEmpty()
                    ? "*"
                    : s.equals( "*" )
                            ? "\"*\""
                            : s ).collect( Collectors.toList() );
        }


        @Override
        public AlgDataTypeField field( AlgDataType rowType, String fieldName ) {
            return rowType.getField( fieldName, caseSensitive, false );
        }


        @Override
        public int frequency( Iterable<String> names, String name ) {
            int n = 0;
            for ( String s : names ) {
                if ( matches( s, name ) ) {
                    ++n;
                }
            }
            return n;
        }


        @Override
        public Set<String> createSet() {
            return isCaseSensitive()
                    ? new LinkedHashSet<>()
                    : new TreeSet<>( String.CASE_INSENSITIVE_ORDER );
        }

    }


    /**
     * Matcher that remembers the requests that were made of it.
     */
    private static class LiberalNameMatcher extends BaseMatcher {

        List<String> matchedNames;


        LiberalNameMatcher() {
            super( false );
        }


        @Override
        protected boolean listMatches( List<String> list0, List<String> list1 ) {
            final boolean b = super.listMatches( list0, list1 );
            if ( b ) {
                matchedNames = ImmutableList.copyOf( list1 );
            }
            return b;
        }


        @Override
        protected void matched( List<String> prefixNames, List<String> names ) {
            matchedNames = ImmutableList.copyOf(
                    Util.startsWith( names, prefixNames )
                            ? Util.skip( names, prefixNames.size() )
                            : names );
        }


        @Override
        public List<String> bestMatch() {
            return matchedNames;
        }

    }

}
