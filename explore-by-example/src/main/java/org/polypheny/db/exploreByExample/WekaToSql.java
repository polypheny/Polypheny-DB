/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.exploreByExample;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class WekaToSql {


    public static String translate( String tree, Map<String, String> nameAndType, boolean includesJoin ) {
        List<String> splitTree = new ArrayList<>( Arrays.asList( tree.replace( "   ", " " ).replace( "  ", " " ).split( "Number of Leaves" )[0].split( "\\n" ) ) );
        splitTree.remove( 0 );
        splitTree.remove( 0 );
        splitTree.remove( 0 );

        List<String> selectedElements = new ArrayList<>();

        for ( String element : splitTree ) {
            if ( !element.contains( "false" ) ) {
                if ( element.contains( ":" ) ) {
                    selectedElements.add( element.split( ":" )[0] );
                } else {
                    selectedElements.add( element );
                }
            }
        }

        splitTree.clear();
        for ( String element : selectedElements ) {
            List<String> elements = new ArrayList<>();
            List<String> temp = new ArrayList<>();
            if ( element.contains( "<=" ) || element.contains( ">" ) ) {
                splitTree.add( element );
            } else if ( element.contains( "=" ) ) {
                elements = Arrays.asList( element.split( "=" ) );

                if ( nameAndType.get( elements.get( 0 ).replaceAll( " ", "" ).replaceAll( "\\|", "" ) ).equals( "VARCHAR" ) ) {
                    temp.add( elements.get( 0 ).replaceAll( " ", "" ) );
                    temp.add( "'" + elements.get( 1 ).substring( 1 ) + "'" );
                    splitTree.add( String.join( " = ", temp ) );
                } else {
                    splitTree.add( element );
                }
            }
        }
        if ( includesJoin ) {
            return "\nAND " + WekaToSql.iterateTee( splitTree );
        } else {
            return "\nWHERE " + WekaToSql.iterateTee( splitTree );
        }
    }


    private static String iterateTee( List<String> splitTree ) {
        List<String> res = new ArrayList<>();
        boolean flag = false;
        List<String> sequence = new ArrayList<>();
        String element;

        for ( String node : splitTree ) {
            if ( node.contains( "|" ) ) {
                sequence.add( node );
                flag = true;
            } else {
                if ( !flag ) {
                    sequence.add( node );
                    flag = true;
                } else {
                    if ( sequence.size() > 0 ) {
                        element = sequence.get( 0 );
                        sequence.remove( 0 );
                        List<String> temp = new ArrayList<>();
                        for ( String el : sequence ) {
                            if ( el.contains( "|" ) ) {
                                temp.add( el.replaceFirst( "\\|", "" ) );
                            } else {
                                temp.add( el );
                            }
                        }

                        if ( temp.size() > 0 ) {
                            res.add( element + "\nAND " + iterateTee( temp ) );
                        } else {
                            res.add( element );
                        }
                        sequence = new ArrayList<>();
                        sequence.add( node );
                    }
                }

            }
        }
        if ( sequence.size() > 0 ) {
            element = sequence.get( 0 );
            sequence.remove( 0 );
            List<String> temp = new ArrayList<>();
            for ( String el : sequence ) {
                if ( el.contains( "|" ) ) {
                    temp.add( el.replace( "|", "" ) );
                } else {
                    temp.add( el );
                }
            }

            if ( temp.size() > 0 ) {
                res.add( element + "\nAND " + iterateTee( temp ) );
            } else {
                res.add( element );
            }
        }
        return "((" + String.join( ") OR (", res ) + "))";
    }

}
