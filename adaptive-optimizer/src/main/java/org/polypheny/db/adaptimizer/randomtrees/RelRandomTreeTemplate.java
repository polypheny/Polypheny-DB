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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.annotations.SerializedName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.randomtrees.except.InvalidBinaryNodeException;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Template for generating random trees. Acts as a wrapper for the random object.
 */
@Slf4j
@Getter
@Setter
public class RelRandomTreeTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    @SerializedName( "tables" )
    private AdaptiveTableRecord[] tableRecords;

    @SerializedName( "schema" )
    private String schemaName;

    @SerializedName( "column_type_map" )
    private HashMap<String, PolyType> columnTypes;

    @SerializedName( "poly_type_equiv_rel" )
    private Set<Pair<String, String>> polyTypePartners;

    @SerializedName( "fk_ref_rel" )
    private Set<Pair<String, String>> references;

    @SerializedName( "binary_ops" )
    private String[] binaryOperators;

    @SerializedName( "unary_ops" )
    private String[] unaryOperators;

    @SerializedName( "join_types" )
    private JoinAlgType[] joinTypes;

    @SerializedName( "filter_ops" )
    private String[] filterOps;

    @SerializedName( "join_ops" )
    private String[] joinOps;

    @SerializedName( "unary_p" )
    private float unaryProbability;

    @SerializedName( "seed" )
    private long seed;

    @SerializedName( "max_h" )
    private int height;

    @SerializedName( "prefer_j" )
    private boolean switchToJoin;

    @SerializedName( "prefer_s" )
    private boolean switchToSetOp;

    @JsonIgnore
    private transient Random random;

    @JsonIgnore
    private transient int counter;

    @JsonIgnore
    private transient int iter;

    public RelRandomTreeTemplate(
            String schemaName,
            int height,
            long seed,
            float unaryProbability,
            boolean switchFlag,
            List<String> binaryOperators,
            List<String> unaryOperators,
            List<JoinAlgType> joinTypes,
            List<String> filterOps,
            List<String> joinOps,
            List<AdaptiveTableRecord> tables,
            Set<Pair<String, String>> polyTypePartners,
            Set<Pair<String, String>> references,
            HashMap<String, PolyType> columnTypes

    ) {
        this.schemaName = schemaName;

        this.height = height;
        this.seed = seed;
        this.unaryProbability = unaryProbability;

        this.switchToJoin = switchFlag;
        this.switchToSetOp = ! switchFlag;

        this.polyTypePartners = polyTypePartners;
        this.references = references;
        this.columnTypes = columnTypes;

        this.binaryOperators = binaryOperators.toArray( String[]::new );
        this.unaryOperators = unaryOperators.toArray( String[]::new );
        this.joinTypes = joinTypes.toArray( JoinAlgType[]::new );
        this.tableRecords = tables.toArray( AdaptiveTableRecord[]::new );
        this.joinOps = joinOps.toArray( String[]:: new );
        this.filterOps = filterOps.toArray( String[]:: new );

        this.random = new Random( seed );
        this.counter = 0;
        this.iter = 0;
    }

    public String nextBinaryOperatorType() {
        return this.binaryOperators[ this.random.nextInt( this.binaryOperators.length ) ];
    }

    public String nextUnaryOperatorType() {
        return this.unaryOperators[ this.random.nextInt( this.unaryOperators.length ) ];
    }

    public boolean nextOperatorIsUnary() {
        return this.random.nextFloat() < this.unaryProbability;
    }

    public Pair<String, AdaptiveTableRecord> nextTable() {
        return new Pair<>( this.schemaName, this.tableRecords[ random.nextInt( this.tableRecords.length ) ] );
    }

    /**
     * Returns the original name of a column.
     */
    private String orgAlias( String columnName ) {
        int i = columnName.indexOf( "___" );
        if ( i == -1 ) {
            return columnName;
        }
        return columnName.substring( i + 3 );
    }

    private List<String> orgAlias( AdaptiveTableRecord record ) {
        return record.getColumns().stream().map( this::orgAlias ).collect( Collectors.toList());
    }

    private Pair<List<String>, List<String>> orgAlias( AdaptiveTableRecord left, AdaptiveTableRecord right) {
        return new Pair<>( this.orgAlias( left ), this.orgAlias( right ) );
    }

    public Pair<String, PolyType> nextColumn( String columnName ) {
        return new Pair<>( columnName, this.columnTypes.get( this.orgAlias( columnName ) ) );
    }

    public Pair<String, PolyType> nextColumn( AdaptiveTableRecord record ) {
        String columnName = record.getColumns().get( this.random.nextInt( record.getColumns().size()) );
        return new Pair<>( columnName, this.columnTypes.get( this.orgAlias( columnName ) ) );
    }

    private List<Pair<String, String>> searchPolyTypePartners( AdaptiveTableRecord record ) {
        List<String> org = this.orgAlias( record );
        return this.polyTypePartners.stream().filter(
                pair -> org.contains( pair.left ) && org.contains( pair.right )
        ).collect( Collectors.toList());
    }

    private List<Pair<String, String>> searchPolyTypePartners( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        Pair<List<String>, List<String>> org = this.orgAlias( left, right );
        return this.polyTypePartners.stream().filter(
                pair -> ( org.left.contains( pair.left ) && org.right.contains( pair.right ))
        ).collect( Collectors.toList());
    }

    private List<Pair<String, String>> searchJoiningForeignKeyReferences( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        Pair<List<String>, List<String>> org = this.orgAlias( left, right );
        return this.references.stream().filter(
                pair -> ( org.left.contains( pair.left ) && org.right.contains( pair.right ) )
        ).collect( Collectors.toList());
    }

    public Pair<String, String> nextJoinColumns( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        List<Pair<String, String>> joinColumnPairs = this.searchJoiningForeignKeyReferences( left, right );
        if ( joinColumnPairs.isEmpty() ) {
            joinColumnPairs = this.searchPolyTypePartners( left, right );
        }
        if ( joinColumnPairs.isEmpty() ) {
            return null; // Todo no join possible here. Maybe find a better way...
        }
        return joinColumnPairs.get( random.nextInt( joinColumnPairs.size() ) );
    }


    /**
     * Tries to match tables for set operations. // Todo make more sophisticated
     */
    public void matchOnColumnTypeSubsets( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        List<Pair<String, String>> polyTypePartners = this.searchPolyTypePartners( left, right );
        List<String> leftPartners = new ArrayList<>();
        List<String> rightPartners = new ArrayList<>();
        polyTypePartners.forEach( pair -> {
            leftPartners.add( pair.left );
            rightPartners.add( pair.right  );
        } );

        left.setColumns( leftPartners );
        right.setColumns( rightPartners );
    }

    // Todo Implement
    public void extendForSetOperation( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        throw new InvalidBinaryNodeException( "Extension not implemented yet", null );
    }

    // Todo Implement
    public void extendForJoinOperation( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        throw new InvalidBinaryNodeException( "Extension not implemented yet", null );
    }

    public int nextInt( int bound ) {
        return this.random.nextInt( bound );
    }

    public PolyType typeOf( String columnName ) {
        return this.columnTypes.get( this.orgAlias( columnName ) );
    }


    /**
     * Checks whether PolyTypes match between two records.
     */
    public boolean haveSamePolyTypes( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        if ( left.getColumns().size() != right.getColumns().size() ) {
            return false;
        }

        for ( int i = 0; i < left.getColumns().size(); i++ ) {
            if ( this.typeOf( left.getColumns().get( i ) ) != this.typeOf( right.getColumns().get( i ) ) ) {
                return false;
            }
        }

        return true;
    }


    /**
     * Returns the "All" option for set operations.
     */
    public boolean nextAll() {
        return true;
    }


    /**
     * Returns true if the generator should switch to join if set operations are not possible. (Will not try to make set ops work)
     */
    public boolean switchToJoinIfSetOpNotViable() {
        return this.switchToJoin;
    }
    /**
     * Returns true if the generator should switch to set ops if join operations are not possible. (Will not try to make join work)
     */
    public boolean switchToSetOpIfJoinNotPossible() {
        return this.switchToSetOp;
    }


    public JoinAlgType nextJoinType() {
        return this.joinTypes[ this.random.nextInt( this.joinTypes.length ) ];
    }

    public String nextJoinOperation() {
        return this.joinOps[ this.random.nextInt( this.joinOps.length ) ];
    }

    public String nextFilterOperation() {
        return this.filterOps[ this.random.nextInt( this.filterOps.length ) ];
    }

    public boolean isMaxDepth( int depth ) {
        return this.height <= depth;
    }

    public String nextUniqueString(int iter) {
        if ( this.iter != iter ) {
            this.iter = iter;
            this.counter = 0;
        }
        this.counter++;
        return "I".repeat( this.counter );
    }

    /**
     * Returns the original seed for this tree generation template.
     */
    public long getSeed() {
        return this.seed;
    }

    /**
     * Copied from https://stackoverflow.com/a/6001407 Returns current seed of random object.
     */
    public long getCurrentSeed() {
        byte[] ba0, ba1, bar;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(new Random(0));
            ba0 = baos.toByteArray();
            baos = new ByteArrayOutputStream(128);
            oos = new ObjectOutputStream(baos);
            oos.writeObject(new Random(-1));
            ba1 = baos.toByteArray();
            baos = new ByteArrayOutputStream(128);
            oos = new ObjectOutputStream(baos);
            oos.writeObject( this.random );
            bar = baos.toByteArray();
        } catch ( IOException e ) {
            throw new RuntimeException("IOException: " + e);
        }
        if (ba0.length != ba1.length || ba0.length != bar.length)
            throw new RuntimeException("bad serialized length");
        int i = 0;
        while (i < ba0.length && ba0[i] == ba1[i]) {
            i++;
        }
        int j = ba0.length;
        while (j > 0 && ba0[j - 1] == ba1[j - 1]) {
            j--;
        }
        if (j - i != 6)
            throw new RuntimeException("6 differing bytes not found");
        // The constant 0x5DEECE66DL is from
        // http://download.oracle.com/javase/6/docs/api/java/util/Random.html .
        return ((bar[i] & 255L) << 40 | (bar[i + 1] & 255L) << 32 |
                (bar[i + 2] & 255L) << 24 | (bar[i + 3] & 255L) << 16 |
                (bar[i + 4] & 255L) << 8 | (bar[i + 5] & 255L)) ^ 0x5DEECE66DL;
    }

    public void setSeed( long seed ) {
        this.random.setSeed( seed );
    }

}
