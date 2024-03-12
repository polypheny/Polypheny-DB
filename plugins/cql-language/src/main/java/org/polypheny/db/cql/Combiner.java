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

package org.polypheny.db.cql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.cql.BooleanGroup.EntityOpsBooleanOperator;
import org.polypheny.db.cql.exception.InvalidMethodInvocation;
import org.polypheny.db.cql.exception.InvalidModifierException;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;


/**
 * Packaging information and algorithm to combine two tables.
 */
@Slf4j
public class Combiner {

    private static final Map<String, Object> modifiersLookupTable = new HashMap<>();

    public final CombinerType combinerType;
    public final String[] joinOnColumns;


    static {
        modifiersLookupTable.put( "null", "both" );
    }


    public Combiner( CombinerType combinerType, String[] joinOnColumns ) {
        this.combinerType = combinerType;
        this.joinOnColumns = joinOnColumns;
    }


    public static Combiner createCombiner( BooleanGroup<EntityOpsBooleanOperator> booleanGroup, EntityIndex left, EntityIndex right ) throws InvalidModifierException {
        log.debug( "Creating Combiner." );
        log.debug( "Setting default values for modifiers." );

        Map<String, Object> modifiers = new HashMap<>( modifiersLookupTable );
        if ( booleanGroup.booleanOperator == EntityOpsBooleanOperator.AND ) {
            modifiers.put( "on", new String[]{ "all" } );
        } else {
            modifiers.put( "on", new String[]{ "none" } );
        }

        try {
            booleanGroup.modifiers.forEach( ( modifierName, modifier ) -> {
                if ( modifierName.equalsIgnoreCase( "on" ) ) {
                    log.debug( "Found 'on' modifier." );
                    modifiers.put( "on", parseOnModifier( modifier.comparator, modifier.modifierValue.trim() ) );
                } else if ( modifier.modifierName.equalsIgnoreCase( "null" ) ) {
                    log.debug( "Found 'null' modifier." );
                    modifiers.put( "null", parseNullModifier( modifier.comparator, modifier.modifierValue.trim() ) );
                } else {
                    log.error( "Invalid modifier for combining tables: {}", modifierName );
                    throw new GenericRuntimeException( "Invalid modifier for combining tables: " + modifierName );
                }
            } );
        } catch ( RuntimeException e ) {
            throw new InvalidModifierException( e.getMessage() );
        }

        CombinerType combinerType = determineCombinerType( booleanGroup.booleanOperator, (String) modifiers.get( "null" ) );
        String[] joinOnColumns = getColumnsToJoinOn( left, right, (String[]) modifiers.get( "on" ) );

        return new Combiner( combinerType, joinOnColumns );
    }


    private static String[] parseOnModifier( Comparator comparator, String modifierValue ) {
        log.debug( "Parsing 'on' modifier." );
        if ( modifierValue.equalsIgnoreCase( "all" ) ) {
            return new String[]{ "all" };
        } else if ( modifierValue.equalsIgnoreCase( "none" ) ) {
            return new String[]{ "none" };
        } else {
            return modifierValue.trim().split( "\\s*,\\s*" );
        }
    }


    private static String parseNullModifier( Comparator comparator, String modifierValue ) {
        log.debug( "Parsing 'null' modifier." );
        if ( modifierValue.equalsIgnoreCase( "left" ) ) {
            return "left";
        } else if ( modifierValue.equalsIgnoreCase( "right" ) ) {
            return "right";
        } else {
            return "both";
        }
    }


    private static CombinerType determineCombinerType( EntityOpsBooleanOperator entityOpsBooleanOperator, String nullValue ) {
        log.debug( "Determining Combiner Type." );
        if ( entityOpsBooleanOperator == EntityOpsBooleanOperator.OR ) {
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


    private static String[] getColumnsToJoinOn( EntityIndex left, EntityIndex right, String[] columnStrs ) throws InvalidModifierException {
        assert columnStrs.length > 0;

        if ( log.isDebugEnabled() ) {
            log.debug( "Getting Columns to Join '{}' and '{}' On.", left.fullyQualifiedName, right.fullyQualifiedName );
        }
        if ( columnStrs.length == 1 ) {
            if ( columnStrs[0].equals( "all" ) ) {
                return getCommonColumns( left, right );
            } else if ( columnStrs[0].equals( "none" ) ) {
                return new String[0];
            }
        }

        LogicalTable leftCatalogTable = left.catalogTable;
        LogicalTable rightCatalogTable = right.catalogTable;
        List<String> columnList = Arrays.asList( columnStrs );

        LogicalRelSnapshot relSnapshot = Catalog.getInstance().getSnapshot().rel();
        List<String> lColumnNames = relSnapshot.getColumns( leftCatalogTable.id ).stream().map( c -> c.name ).toList();
        List<String> rColumnNames = relSnapshot.getColumns( rightCatalogTable.id ).stream().map( c -> c.name ).toList();
        if ( !new HashSet<>( lColumnNames ).containsAll( columnList ) || !new HashSet<>( rColumnNames ).containsAll( columnList ) ) {
            log.error( "Invalid Modifier Values. Cannot join tables '{}' and '{}' on columns {}",
                    leftCatalogTable.name, rightCatalogTable.name, columnList );
            throw new InvalidModifierException( "Invalid Modifier Values. Cannot join tables '" +
                    leftCatalogTable.name + "' and '" + rightCatalogTable.name + "' on columns " + columnList );
        }

        return columnStrs;
    }


    private static String[] getCommonColumns( EntityIndex table1, EntityIndex table2 ) {
        // TODO: Create a cache and check if in cache.

        if ( log.isDebugEnabled() ) {
            log.debug( "Getting Common Columns between '{}' and '{}'.", table1.fullyQualifiedName, table2.fullyQualifiedName );
        }
        LogicalRelSnapshot relSnapshot = Catalog.getInstance().getSnapshot().rel();
        List<String> table1Columns = relSnapshot.getColumns( table1.catalogTable.id ).stream().map( c -> c.name ).toList();
        List<String> table2Columns = relSnapshot.getColumns( table2.catalogTable.id ).stream().map( c -> c.name ).toList();

        return table1Columns.stream().filter( table2Columns::contains ).toArray( String[]::new );
    }


    public AlgBuilder combine( AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        log.debug( "Combining." );
        try {
            if ( combinerType.isJoinType() ) {
                if ( joinOnColumns.length == 0 ) {
                    return algBuilder.join( combinerType.convertToJoinRelType(), rexBuilder.makeLiteral( true ) );
                } else {
                    return algBuilder.join( combinerType.convertToJoinRelType(), joinOnColumns );
                }
            } else {
//                TODO: Implement SetOpsType Combiners. (Union, Intersection, etc.)
                throw new GenericRuntimeException( "Set Ops Type Combiners have not been implemented." );
            }
        } catch ( InvalidMethodInvocation e ) {
            throw new GenericRuntimeException( "This exception would never be thrown since we have checked if the combiner isJoinType.", e );
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


        public JoinAlgType convertToJoinRelType() throws InvalidMethodInvocation {
            log.debug( "Converting to JoinRelType." );
            if ( !isJoinType ) {
                log.error( "Cannot convert non-JoinType Combiner to JoinRelType." );
                throw new InvalidMethodInvocation( "Cannot convert non-JoinType Combiner to JoinRelType." );
            }
            if ( this == JOIN_FULL ) {
                return JoinAlgType.FULL;
            } else if ( this == JOIN_INNER ) {
                return JoinAlgType.INNER;
            } else if ( this == JOIN_LEFT ) {
                return JoinAlgType.LEFT;
            } else {
                return JoinAlgType.RIGHT;
            }
        }
    }

}
