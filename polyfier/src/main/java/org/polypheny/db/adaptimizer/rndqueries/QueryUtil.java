/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.rndqueries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.exceptions.AdaptiveOptException;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownTableException;

public abstract class QueryUtil {

    public static QueryTemplate createCustomTemplate( HashMap<String, String> parameters ) throws NumberFormatException, NullPointerException {
        String schema = parameters.get( "Schema" );

        final ArrayList<Throwable> errors = new ArrayList<>();
        List<CatalogTable> catalogTables = Arrays.stream( parameters.get( "Tables" ).split( " " ) ).map( t -> {
            try {
                return AdaptiveOptimizerImpl.getCatalog().getTable( Catalog.defaultDatabaseId, schema, t );
            } catch ( UnknownTableException e ) {
                errors.add( e );
                return null;
            }
        } ).collect( Collectors.toList() );

        if ( ! errors.isEmpty() ) {
            throw new AdaptiveOptException("Table not found...", errors.get( 0 ));
        }

        int maxHeight = Integer.parseInt( parameters.get( "Tree Height" ) );
        long seed = Long.parseLong( parameters.get( "Seed" ) );
        float unaryP = Float.parseFloat( parameters.get( "Unary Probability (Float)" ) );
        int unionFreq = Integer.parseInt( parameters.get( "Union Frequency (Int)" ) );
        int minusFreq = Integer.parseInt( parameters.get( "Minus Frequency (Int)" ) );
        int intersectFreq = Integer.parseInt( parameters.get( "Intersect Frequency (Int)" ) );
        int joinFreq = Integer.parseInt( parameters.get( "Join Frequency (Int)" ) );
        int projectFreq = Integer.parseInt( parameters.get( "Project Frequency (Int)" ) );
        int sortFreq = Integer.parseInt( parameters.get( "Sort Frequency (Int)" ) );
        int filterFreq = Integer.parseInt( parameters.get( "Filter Frequency (Int)" ) );

        return RelQueryTemplate.builder( AdaptiveOptimizerImpl.getCatalog(), catalogTables )
                .schemaName( schema )
                .seed( seed )
                .random( new Random( seed ) )
                .unaryProbability( unaryP )
                .addBinaryOperator( "Join", joinFreq )
                .addBinaryOperator( "Union", unionFreq )
                .addBinaryOperator( "Intersect", intersectFreq )
                .addBinaryOperator( "Minus", minusFreq )
                .addUnaryOperator( "Sort", sortFreq )
                .addUnaryOperator( "Project", projectFreq )
                .addUnaryOperator( "Filter", filterFreq )
                .height( maxHeight )
                .addFilterOperator( "<>" )
                .addJoinOperator( "=" )
                .addJoinType( JoinAlgType.FULL )
                .addJoinType( JoinAlgType.INNER )
                .addJoinType( JoinAlgType.LEFT )
                .addJoinType( JoinAlgType.RIGHT )
                .build();
    }

}
