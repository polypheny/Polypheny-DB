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

package org.polypheny.db.statistic.exploreByExample;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class WekaToSql {


    public static String translate( String tree ) {
        List<String> splitedTree = new ArrayList<>( Arrays.asList( tree.replace( "   ", " " ).replace( "  ", " " ).split( "Number of Leaves" )[0].split( "\\n" ) ) );
        splitedTree.remove( 0 );
        splitedTree.remove( 0 );
        splitedTree.remove( 0 );

        List<String> selectedElements = new ArrayList<>(  );

        for(String element: splitedTree){
            if(!element.contains( "true" )) {
                if(element.contains( ":" )){
                    selectedElements.add(element.split( ":" )[0]);
                }else {
                    selectedElements.add( element );
                }
            }
        }

        System.out.println( selectedElements );

        return "\nWHERE " + WekaToSql.iterateTee( selectedElements );
    }


    private static String iterateTee( List<String> splitedTree ) {
        List<String> res = new ArrayList<>();
        boolean flag = false;
        List<String> sequence = new ArrayList<>();
        String element;

        for ( String node : splitedTree ) {
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
                                temp.add( el.replace( "|", "" ) );
                            } else {
                                temp.add( el );
                            }
                        }

                        if( temp.size() > 0 ){
                            res.add( element + " AND " + iterateTee( temp ) );
                        }else {
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

            if( temp.size() > 0 ){
                res.add( element + " AND " + iterateTee( temp ) );
            }else {
                res.add( element );
            }

        }
        if( res.size() > 2) {
            return "((" + String.join( ") OR (", res ) + "))";
        }else {
            return "(" + String.join( " OR ", res ) + ")";
        }
    }

}
