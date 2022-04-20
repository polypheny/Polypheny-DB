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

package org.polypheny.db.adaptimizer.randomtrees;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


public class RelRandomTreeTemplate implements RandomTreeTemplate {
    private String schemaName;
    private HashMap<String, PolyType> columnTypes;
    private final ArrayList<String> operatorDistribution;
    private final Set<Pair<String, String>> polyTypePartners;
    private final Set<Pair<String, String>> references;
    private final List<AdaptiveTableRecord> tableRecords;
    private final Random random;
    private int height;


    public RelRandomTreeTemplate(
            String schemaName,
            HashMap<String, PolyType> columnTypes,
            ArrayList<String> operatorDistribution,
            int height,
            Set<Pair<String, String>> polyTypePartners,
            Set<Pair<String, String>> references,
            List<AdaptiveTableRecord> tableRecords
    ) {
        this.schemaName = schemaName;
        this.columnTypes = columnTypes;
        this.operatorDistribution = operatorDistribution;
        this.height = height;
        this.polyTypePartners = polyTypePartners;
        this.references = references;
        this.tableRecords = tableRecords;
        this.random = new Random();
    }

    public int getHeight() {
        return this.height;
    }

    @Override
    public String nextOperator() {
        return this.operatorDistribution.get( random.nextInt( this.operatorDistribution.size() ) );
    }

    @Override
    public Pair<String, TableRecord> nextTable() {
        return new Pair<>( this.schemaName, this.tableRecords.get( random.nextInt( this.tableRecords.size() ) ) );
    }

    @Override
    public Pair<String, PolyType> nextColumn( TableRecord a ) {
        String columnName = a.getColumns().get( this.random.nextInt( a.getColumns().size()) );
        return new Pair<>( columnName, this.columnTypes.get( columnName ) );
    }

    private List<Pair<String, String>> searchPolyTypePartners( TableRecord a ) {
        return this.polyTypePartners.stream().filter(
                pair -> a.getColumns().contains( pair.left ) && a.getColumns().contains( pair.right )
        ).collect( Collectors.toList());
    }

    private List<Pair<String, String>> searchPolyTypePartners( TableRecord a, TableRecord b ) {
        return this.polyTypePartners.stream().filter(
                pair -> ( a.getColumns().contains( pair.left ) && b.getColumns().contains( pair.right ) )
        ).collect( Collectors.toList());
    }

    private List<Pair<String, String>> searchJoiningForeignKeyReferences( TableRecord a, TableRecord b ) {
        return this.references.stream().filter(
                pair -> ( a.getColumns().contains( pair.left ) && b.getColumns().contains( pair.right ) )
        ).collect( Collectors.toList());
    }

    @Override
    public Pair<String, String> nextPolyTypePartners( TableRecord table ) {
        List<Pair<String, String>> polyTypeColumnPairs = this.searchPolyTypePartners( table );
        return polyTypeColumnPairs.get( random.nextInt( polyTypeColumnPairs.size() ) );
    }

    @Override
    public Pair<String, String> nextPolyTypePartners( TableRecord tableA, TableRecord tableB  ) {
        List<Pair<String, String>> polyTypeColumnPairs = this.searchPolyTypePartners( tableA, tableB );
        return polyTypeColumnPairs.get( random.nextInt( polyTypeColumnPairs.size() ) );
    }

    @Override
    public Pair<String, String> nextJoinColumnsWithForeignKeys( TableRecord tableA, TableRecord tableB ) {
        List<Pair<String, String>> joinColumnPairs = this.searchJoiningForeignKeyReferences( tableA, tableB );
        return joinColumnPairs.get( random.nextInt( joinColumnPairs.size() ) );
    }

    @Override
    public Pair<String, String> nextJoinColumns( TableRecord tableA, TableRecord tableB ) {
        List<Pair<String, String>> joinColumnPairs = this.searchJoiningForeignKeyReferences( tableA, tableB );
        if ( joinColumnPairs.isEmpty() ) {
            joinColumnPairs = this.searchPolyTypePartners( tableA, tableB );
        }
        if ( joinColumnPairs.isEmpty() ) {
            return null; // Todo no join possible here. Maybe find a better way...
        }
        return joinColumnPairs.get( random.nextInt( joinColumnPairs.size() ) );
    }

    public int nextInt( int bound ) {
        return this.random.nextInt( bound );
    }

}
