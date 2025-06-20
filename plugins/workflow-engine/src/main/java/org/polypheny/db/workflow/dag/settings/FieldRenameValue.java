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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class FieldRenameValue implements SettingValue {

    List<RenameRule> rules;
    SelectMode mode;
    boolean ignoreCase; // treat source case insensitive, not relevant for index mode


    public boolean hasDuplicates() {
        Set<String> sources = new HashSet<>();
        for ( RenameRule rule : rules ) {
            if ( sources.contains( rule.getSource() ) ) {
                return true;
            }
            sources.add( rule.getSource() );
        }
        return false;
    }


    public boolean hasEmptyRule() {
        return rules.stream().anyMatch( RenameRule::isEmpty );
    }


    public void validateRegex() throws PatternSyntaxException {
        for ( RenameRule rule : rules ) {
            rule.getPattern( ignoreCase );
        }
    }


    public void validateIndex() throws NumberFormatException {
        for ( RenameRule rule : rules ) {
            int i = rule.asIndex();
            if ( i < 0 ) {
                throw new NumberFormatException( "Index out of range" );
            }
        }
    }


    /**
     * Maps each inName that gets renamed to its outName.
     * Names that are not renamed are not part of the map.
     */
    public Map<String, String> getMapping( List<String> inNames ) {
        Set<String> remaining = new HashSet<>( inNames );
        Map<String, String> mapping = new HashMap<>();

        for ( RenameRule rule : rules ) {
            switch ( mode ) {
                case EXACT -> {
                    for ( String inName : Set.copyOf( remaining ) ) {
                        String renamed = rule.applyConstantRule( inName, ignoreCase );
                        if ( renamed != null ) {
                            mapping.put( inName, renamed );
                            remaining.remove( inName );
                            break; // no duplicate names are possible
                        }
                    }
                }
                case REGEX -> {
                    for ( String inName : Set.copyOf( remaining ) ) {
                        String renamed = rule.applyRegexRule( inName, ignoreCase );
                        if ( renamed != null ) {
                            mapping.put( inName, renamed );
                            remaining.remove( inName );
                        }
                    }
                }
                case INDEX -> {
                    int i = rule.asIndex();
                    if ( i >= inNames.size() ) {
                        continue;
                    }
                    String inName = inNames.get( i );
                    mapping.put( inName, rule.applyIndexRule( inName ) );
                }
            }
        }
        return mapping;
    }


    public Set<String> getRenamedSet( Set<String> inNames ) {
        if ( mode == SelectMode.INDEX ) {
            throw new GenericRuntimeException( "Cannot select names by index when given a set" );
        }
        Map<String, String> map = getMapping( new ArrayList<>( inNames ) );
        Set<String> renamed = new HashSet<>();
        for ( String name : inNames ) {
            renamed.add( map.getOrDefault( name, name ) );
        }
        return Collections.unmodifiableSet( renamed );
    }


    /**
     * Attempts to rename the given string.
     *
     * @return the renamed name or null if it wasn't renamed.
     */
    public String rename( String name ) {
        return switch ( mode ) {
            case EXACT -> {
                for ( RenameRule rule : rules ) {
                    String renamed = rule.applyConstantRule( name, ignoreCase );
                    if ( renamed != null ) {
                        yield renamed;
                    }
                }
                yield null;
            }
            case REGEX -> {
                for ( RenameRule rule : rules ) {
                    String renamed = rule.applyRegexRule( name, ignoreCase );
                    if ( renamed != null ) {
                        yield renamed;
                    }
                }
                yield null;
            }
            default -> throw new GenericRuntimeException( "Unsupported mode " + mode );
        };
    }


    @Value
    public static class RenameRule {

        String source; // either constant, regex (with optional match groups) or index
        String replacement; // represents a replace string for replaceAll. Use $0 to reference the entire input, $1 for the first group in case of regex etc.

        @JsonIgnore
        @NonFinal
        Pattern compiledPattern;

        @JsonIgnore
        @NonFinal
        Integer index;

        @JsonIgnore
        static Pattern ALL_MATCH = Pattern.compile( ".+" );


        /*@JsonCreator
        public RenameRule( int sourceIdx, String replacement ) {
            this( String.valueOf( sourceIdx ), replacement, false );
        }


        @JsonCreator
        public RenameRule( String source, String replacement ) {
            this( source, replacement, false );
        }


        @JsonCreator
        public RenameRule( String source, String replacement, boolean caseInsensitive ) {
            this.source = source;
            this.replacement = replacement;
            this.caseInsensitive = caseInsensitive;
        }*/


        @JsonIgnore
        public Pattern getPattern( boolean ignoreCase ) {
            if ( compiledPattern == null ) {
                if ( ignoreCase ) {
                    compiledPattern = Pattern.compile( source, Pattern.CASE_INSENSITIVE );
                } else {
                    compiledPattern = Pattern.compile( source );
                }
            }
            return compiledPattern;
        }


        @JsonIgnore
        public boolean isEmpty() {
            return source.isEmpty() || replacement.isEmpty();
        }


        public int asIndex() throws NumberFormatException {
            if ( index == null ) {
                index = Integer.parseInt( source );
            }
            return index;
        }


        public String applyConstantRule( String inName, boolean ignoreCase ) {
            if ( inName.equals( source ) || (ignoreCase && inName.equalsIgnoreCase( source )) ) {
                return ALL_MATCH.matcher( inName ).replaceAll( replacement );
            }
            return null;
        }


        public String applyRegexRule( String inName, boolean ignoreCase ) {
            Matcher matcher = getPattern( ignoreCase ).matcher( inName );
            if ( matcher.find() ) {
                return matcher.replaceAll( replacement );
            }
            return null;
        }


        public String applyIndexRule( String inName ) {
            return ALL_MATCH.matcher( inName ).replaceAll( replacement );
        }

    }


}
