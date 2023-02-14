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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.annotations.SerializedName;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.exceptions.InvalidBinaryNodeException;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Template for generating random queries. Acts as a wrapper for the random object.
 */
@Slf4j
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class RelQueryTemplate implements Serializable, QueryTemplate {
    private static final long serialVersionUID = 1L;

    @SerializedName( "tables" )
    private List<AdaptiveTableRecord> tableRecords;

    @Default
    @SerializedName( "schema" )
    private String schemaName = "public";

    @SerializedName( "column_type_map" )
    private HashMap<String, PolyType> columnTypes;

    @SerializedName( "poly_type_equiv_rel" )
    private Set<Pair<String, String>> polyTypePartners;

    @SerializedName( "fk_ref_rel" )
    private Set<Pair<String, String>> references;

    @Default
    @SerializedName( "binary_ops" )
    private List<String> binaryOperators = new ArrayList<>();

    @Default
    @SerializedName( "unary_ops" )
    private List<String> unaryOperators = new ArrayList<>();

    @Default
    @SerializedName( "join_types" )
    private List<JoinAlgType> joinTypes = new ArrayList<>();

    @Default
    @SerializedName( "filter_ops" )
    private List<String> filterOps = new ArrayList<>();

    @Default
    @SerializedName( "join_ops" )
    private List<String> joinOps = new ArrayList<>();

    @Default
    @SerializedName( "unary_p" )
    private float unaryProbability = 0.5f;

    @Getter(AccessLevel.NONE)
    @SerializedName( "seed" )
    private long seed;

    @Default
    @SerializedName( "max_h" )
    private int height = 5;

    @Default
    @SerializedName( "prefer_j" )
    private boolean switchToJoin = false;

    @Default
    @SerializedName( "prefer_s" )
    private boolean switchToSetOp = true;

    @Default
    @SerializedName( "proj_sort_workaround" )
    private boolean projectSortColumn = true;

    @JsonIgnore
    private transient Random random;

    @Default
    @JsonIgnore
    private transient int counter = 0;

    @Default
    @JsonIgnore
    private transient int iter = 0;


    // -------------------------------------------------
    //              Builder Tweaks
    // -------------------------------------------------


    // Overwrite builder function in lombok
    public static RelQueryTemplateBuilder builder( Catalog catalog, List<CatalogTable> catalogTables ) {
        return new RelQueryTemplateBuilder()
                .polyTypePartners( QueryTemplateUtil.getPolyTypePartners( catalog, catalogTables ) )
                .tableRecords( QueryTemplateUtil.getTableRecords( catalog, catalogTables ) )
                .columnTypes( QueryTemplateUtil.getColumnTypes( catalog, catalogTables ) )
                .references( QueryTemplateUtil.getForeignKeyReferences( catalog, catalogTables ) );
    }


    // Overwrite functions of the lombok Builder
    public static class RelQueryTemplateBuilder {

        // add functions

        public RelQueryTemplateBuilder addBinaryOperator( String binaryOperator, int frequency ) {
            if ( binaryOperators$value == null ) {
                binaryOperators$value = new ArrayList<>();
            }
            for ( int i = 0; i < frequency; i++ ) {
                binaryOperators$value.add( binaryOperator );
            }
            binaryOperators$set = true;
            return this;
        }

        public RelQueryTemplateBuilder addUnaryOperator( String unaryOperator, int frequency ) {
            if ( unaryOperators$value == null ) {
                unaryOperators$value = new ArrayList<>();
            }
            for ( int i = 0; i < frequency; i++ ) {
                unaryOperators$value.add( unaryOperator );
            }
            unaryOperators$set = true;
            return this;
        }

        public RelQueryTemplateBuilder setSwitch( boolean toSetOp ) {
            this.switchToSetOp$value = toSetOp;
            this.switchToJoin$value = ! toSetOp;
            return this;
        }

        // hide functions

        private RelQueryTemplateBuilder polyTypePartners( Set<Pair<String, String>> polyTypePartners ) {
            this.polyTypePartners = polyTypePartners;
            return this;
        }

        private RelQueryTemplateBuilder references( Set<Pair<String, String>> references ) {
            this.references = references;
            return this;
        }

        private RelQueryTemplateBuilder columnTypes( HashMap<String, PolyType> columnTypes ) {
            this.columnTypes = columnTypes;
            return this;
        }

        private RelQueryTemplateBuilder tableRecords( List<AdaptiveTableRecord> tableRecords ) {
            this.tableRecords = tableRecords;
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder unaryOperators( List<String> unaryOperators ) {
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder binaryOperators( List<String> binaryOperators ) {
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder joinOps( List<String> joinOps ) {
            return this;
        }

        @SuppressWarnings( "unused" )
        public RelQueryTemplateBuilder addJoinOperator( String joinOp ) {
            if ( joinOps$value == null ) {
                joinOps$value = new ArrayList<>();
            }
            joinOps$value.add( joinOp );
            joinOps$set = true;
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder filterOps( List<String> filterOps ) {
            return this;
        }

        @SuppressWarnings( "unused" )
        public RelQueryTemplateBuilder addFilterOperator( String filterOp ) {
            if ( filterOps$value == null ) {
                filterOps$value = new ArrayList<>();
            }
            filterOps$value.add( filterOp );
            filterOps$set = true;
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder joinTypes( List<JoinAlgType> joinTypes ) {
            return this;
        }

        @SuppressWarnings( "unused" )
        public RelQueryTemplateBuilder addJoinType( JoinAlgType joinAlgType ) {
            if ( joinTypes$value == null ) {
                joinTypes$value = new ArrayList<>();
            }
            joinTypes$value.add( joinAlgType );
            joinTypes$set = true;
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder counter( int counter ) {
            return this;
        }

        @SuppressWarnings( "unused" )
        private RelQueryTemplateBuilder iter( int iter ) {
            return this;
        }

    }

    /**
     * Auxiliary Class for readability.
     */
    private static class Choice<T> {
        public T nextOf( Random random, List<T> list ) {
            return list.get( random.nextInt( list.size() ) );
        }
    }

    @Override
    public boolean nextAll() {
        return true;
    }

    @Override
    public int nextInt( int bound ) {
        return this.random.nextInt( bound );
    }

    @Override
    public long nextLong() {
        return this.random.nextLong();
    }

    @Override
    public String nextBinaryOperatorType() {
        return new Choice<String>().nextOf( this.random, this.binaryOperators );
    }

    @Override
    public String nextUnaryOperatorType() {
        return new Choice<String>().nextOf( this.random, this.unaryOperators );
    }

    @Override
    public Pair<String, AdaptiveTableRecord> nextTable() {
        return new Pair<>(
                this.schemaName,
                new Choice<AdaptiveTableRecord>().nextOf( this.random, this.tableRecords )
        );
    }

    @Override
    public boolean nextOperatorIsUnary() {
        return this.random.nextFloat() < this.unaryProbability;
    }

    @Override
    public Pair<String, PolyType> nextColumn( String columnName ) {
        return new Pair<>( columnName, this.columnTypes.get( this.orgAlias( columnName ) ) );
    }

    @Override
    public Pair<String, PolyType> nextColumn( AdaptiveTableRecord record ) {
        String columnName = new Choice<String>().nextOf( this.random, record.getColumns() );
        return new Pair<>( columnName, this.columnTypes.get( this.orgAlias( columnName ) ) );
    }

    @Override
    public JoinAlgType nextJoinType() {
        return new Choice<JoinAlgType>().nextOf( this.random, this.joinTypes );
    }

    @Override
    public String nextJoinOperation() {
        return new Choice<String>().nextOf( this.random, this.joinOps );
    }

    @Override
    public String nextFilterOperation() {
        return new Choice<String>().nextOf( this.random, this.filterOps );
    }

    @Override
    public String nextUniqueString( int iter ) {
        if ( this.iter != iter ) {
            this.iter = iter;
            this.counter = 0;
        }
        this.counter++;
        return "I".repeat( this.counter );
    }


    private String orgAlias( String columnName ) {
        // Returns the original Alias of a column name...
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


    private List<Pair<String, String>> searchPolyTypePartners( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        Pair<List<String>, List<String>> org = this.orgAlias( left, right );
        return this.polyTypePartners.stream().filter(
                pair -> ( org.left.contains( pair.left ) && org.right.contains( pair.right ))
        ).collect( Collectors.toList() );
    }

    private List<Pair<String, String>> searchJoiningForeignKeyReferences( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        Pair<List<String>, List<String>> org = this.orgAlias( left, right );
        return this.references.stream().filter(
                pair -> ( org.left.contains( pair.left ) && org.right.contains( pair.right ) )
        ).collect( Collectors.toList() );
    }


    @Override
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


    @Override
    public void matchOnColumnTypeSubsets( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        // Todo make more sophisticated
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


    @Override
    public void extendForSetOperation( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        // Todo Implement
        throw new InvalidBinaryNodeException( "Extension not implemented yet", null );
    }


    @Override
    public void extendForJoinOperation( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        // Todo Implement
        throw new InvalidBinaryNodeException( "Extension not implemented yet", null );
    }


    @Override
    public PolyType typeOf( String columnName ) {
        return this.columnTypes.get( this.orgAlias( columnName ) );
    }


    @Override
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


    @Override
    public boolean switchToJoinIfSetOpNotViable() {
        return this.switchToJoin;
    }


    @Override
    public boolean switchToSetOpIfJoinNotPossible() {
        return this.switchToSetOp;
    }


    @Override
    public boolean projectSortColumnWorkaround() { return this.projectSortColumn; }


    @Override
    public boolean isMaxDepth( int depth ) {
        return this.height <= depth;
    }


    @Override
    public Random getRnd() {
        return this.random;
    }


}
