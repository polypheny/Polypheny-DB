/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.cql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.cql.BooleanGroup.TableOpsBooleanOperator;
import org.polypheny.cql.exception.InvalidMethodInvocation;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.RelBuilder;


/**
 * Packaging information and algorithm to combine two tables.
 */
public class Combiner {

    private static final Map<String, Object> modifiersLookupTable = new HashMap<>();


    static {
        modifiersLookupTable.put( "null", "both" );
    }


    public final CombinerType combinerType;
    public final String[] joinOnColumns;


    public Combiner( CombinerType combinerType, String[] joinOnColumns ) {
        this.combinerType = combinerType;
        this.joinOnColumns = joinOnColumns;
    }


    public static Combiner createCombiner( BooleanGroup<TableOpsBooleanOperator> booleanGroup,
            TableIndex left, TableIndex right ) {

        Map<String, Object> modifiers = new HashMap<>( modifiersLookupTable );
        if ( booleanGroup.booleanOperator == TableOpsBooleanOperator.AND ) {
            modifiers.put( "on", new String[]{ "all" } );
        } else {
            modifiers.put( "on", new String[]{ "none" } );
        }

        booleanGroup.modifiers.forEach( ( modifierName, modifier ) -> {
            if ( modifierName.equalsIgnoreCase( "on" ) ) {
                modifiers.put( "on", parseOnModifier( modifier.comparator, modifier.modifierValue.trim() ) );
            } else if ( modifier.modifierName.equalsIgnoreCase( "null" ) ) {
                modifiers.put( "null", parseNullModifier( modifier.comparator, modifier.modifierValue.trim() ) );
            }
        } );

        CombinerType combinerType =
                determineCombinerType( booleanGroup.booleanOperator, (String) modifiers.get( "null" ) );
        String[] joinOnColumns = getColumnsToJoinOn( left, right, (String[]) modifiers.get( "on" ) );

        return new Combiner( combinerType, joinOnColumns );
    }


    private static String[] parseOnModifier( Comparator comparator, String modifierValue ) {

        if ( modifierValue.equalsIgnoreCase( "all" ) ) {
            return new String[]{ "all" };
        } else if ( modifierValue.equalsIgnoreCase( "none" ) ) {
            return new String[]{ "none" };
        } else {
            return modifierValue.trim().split( "\\s*,\\s*" );
        }

    }


    private static String parseNullModifier( Comparator comparator, String modifierValue ) {

        if ( modifierValue.equalsIgnoreCase( "left" ) ) {
            return "left";
        } else if ( modifierValue.equalsIgnoreCase( "right" ) ) {
            return "right";
        } else {
            return "both";
        }

    }


    private static CombinerType determineCombinerType( TableOpsBooleanOperator tableOpsBooleanOperator, String nullValue ) {
        if ( tableOpsBooleanOperator == TableOpsBooleanOperator.OR ) {
            if ( nullValue.equals( "both" ) ) {
                return CombinerType.JOIN_FULL;
            } else if ( nullValue.equals( "left" ) ) {
                return CombinerType.JOIN_RIGHT;
            } else {
                return CombinerType.JOIN_LEFT;
            }
        } else {
            return CombinerType.JOIN_INNER;
        }
    }


    private static String[] getColumnsToJoinOn( TableIndex left, TableIndex right, String[] columnStrs ) {

        assert columnStrs.length > 0;

        if ( columnStrs.length == 1 ) {
            if ( columnStrs[0].equals( "all" ) ) {
                return getCommonColumns( left.catalogTable, right.catalogTable );
            } else if ( columnStrs[0].equals( "none" ) ) {
                return new String[0];
            }
        }

        CatalogTable leftCatalogTable = left.catalogTable;
        CatalogTable rightCatalogTable = right.catalogTable;
        List<String> columnList = Arrays.asList( columnStrs );

        if ( !leftCatalogTable.getColumnNames().containsAll( columnList ) ||
                !rightCatalogTable.getColumnNames().containsAll( columnList ) ) {

            throw new RuntimeException( "Cannot join tables '" + leftCatalogTable.name + "' and '" +
                    rightCatalogTable.name + "' on columns " + columnList );
        }

        return columnStrs;
    }


    private static String[] getCommonColumns( CatalogTable table1, CatalogTable table2 ) {
        // TODO: Create a cache and check if in cache.

        List<String> table1Columns = table1.getColumnNames();
        List<String> table2Columns = table2.getColumnNames();

        return table1Columns.stream().filter( table2Columns::contains ).toArray( String[]::new );
    }


    public RelBuilder combine( RelBuilder relBuilder, RexBuilder rexBuilder ) {
        try {
            if ( combinerType.isJoinType() ) {
                if ( joinOnColumns.length == 0 ) {
                    return relBuilder.join( combinerType.convertToJoinRelType(), rexBuilder.makeLiteral( true ) );
                } else {
                    return relBuilder.join( combinerType.convertToJoinRelType(), joinOnColumns );
                }
            } else {
                return relBuilder;
            }
        } catch ( InvalidMethodInvocation e ) {
            throw new RuntimeException( "This exception would never be thrown since we have checked if the combiner"
                    + " isJoinType." );
        }
    }


    @Override
    public String toString() {
        return combinerType.name() + " ON " + Arrays.toString( joinOnColumns );
    }


    /**
     * Type of the combiner.
     *
     * Broadly, there are two ways to combine tables. 1. Join and 2. Set Operations.
     * Currently, only JoinType combiners are supported. Support for SetOpsType
     * combiners will be added later.
     */
    public enum CombinerType {
        JOIN_FULL( true ),
        JOIN_INNER( true ),
        JOIN_LEFT( true ),
        JOIN_RIGHT( true );


        private final boolean isJoinType;


        CombinerType( boolean isJoinType ) {
            this.isJoinType = isJoinType;
        }


        public boolean isJoinType() {
            return isJoinType;
        }


        public JoinRelType convertToJoinRelType() throws InvalidMethodInvocation {
            if ( !isJoinType ) {
                throw new InvalidMethodInvocation( "Invalid method call to convert to JoinRelType." );
            }
            if ( this == JOIN_FULL ) {
                return JoinRelType.FULL;
            } else if ( this == JOIN_INNER ) {
                return JoinRelType.INNER;
            } else if ( this == JOIN_LEFT ) {
                return JoinRelType.LEFT;
            } else {
                return JoinRelType.RIGHT;
            }
        }
    }

}
