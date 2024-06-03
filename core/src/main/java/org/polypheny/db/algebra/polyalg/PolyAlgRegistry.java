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

package org.polypheny.db.algebra.polyalg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.common.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.algebra.logical.relational.LogicalSortExchange;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.OperatorTag;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamTag;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.SimpleType;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.IntArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.catalog.logistic.DataModel;

public class PolyAlgRegistry {

    private static final Map<Class<? extends AlgNode>, PolyAlgDeclaration> declarations = new HashMap<>();
    private static final Map<String, Class<? extends AlgNode>> classes = new HashMap<>();
    private static ObjectNode serialized = null;


    static {
        populateDeclarationsMap();
        populateClassesMap();
    }


    private static void populateDeclarationsMap() {
        // logical operators can also be used as allocation operators
        ImmutableList<OperatorTag> logAllTags = ImmutableList.of( OperatorTag.LOGICAL, OperatorTag.ALLOCATION );
        ImmutableList<OperatorTag> logAllProTags = ImmutableList.of( OperatorTag.LOGICAL, OperatorTag.ALLOCATION, OperatorTag.ADVANCED );

        // RELATIONAL
        declarations.put( LogicalRelProject.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelProject::create ).model( DataModel.RELATIONAL )
                .opName( "REL_PROJECT" ).opAliases( List.of( "PROJECT", "P", "REL_PROJECT#", "PROJECT#" ) ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "projects" ).tags( List.of( ParamTag.ALIAS, ParamTag.HIDE_TRIVIAL ) ).multiValued( 1 ).type( ParamType.REX ).build() )
                .build() );
        declarations.put( LogicalRelScan.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelScan::create ).model( DataModel.RELATIONAL )
                .opName( "REL_SCAN" ).opAlias( "SCAN" ).numInputs( 0 ).opTags( logAllTags )
                .param( Parameter.builder().name( "entity" ).alias( "table" ).type( ParamType.ENTITY ).build() )
                .build() );
        declarations.put( LogicalRelViewScan.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelViewScan::create ).model( DataModel.RELATIONAL )
                .opName( "REL_VIEW_SCAN" ).opAlias( "VIEW_SCAN" ).numInputs( 0 ).opTags( logAllTags )
                .param( Parameter.builder().name( "entity" ).alias( "table" ).type( ParamType.ENTITY ).build() )
                .build() );
        declarations.put( LogicalRelFilter.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelFilter::create ).model( DataModel.RELATIONAL )
                .opName( "REL_FILTER" ).opAlias( "FILTER" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "condition" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .param( Parameter.builder().name( "variables" ).type( ParamType.CORR_ID ).simpleType( SimpleType.HIDDEN ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalRelAggregate.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelAggregate::create ).model( DataModel.RELATIONAL )
                .opName( "REL_AGGREGATE" ).opAliases( List.of( "AGGREGATE", "AGG" ) ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "group" ).type( ParamType.FIELD ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )  // select count(*) has no group
                .param( Parameter.builder().name( "groups" ).type( ParamType.FIELD ).simpleType( SimpleType.HIDDEN ).multiValued( 2 ).defaultValue( ListArg.NESTED_EMPTY ).build() )
                .param( Parameter.builder().name( "aggregates" ).alias( "aggs" ).type( ParamType.AGGREGATE ).simpleType( SimpleType.SIMPLE_AGG ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalRelMinus.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelMinus::create ).model( DataModel.RELATIONAL )
                .opName( "REL_MINUS" ).opAlias( "MINUS" ).numInputs( -1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "all" ).type( ParamType.BOOLEAN ).simpleType( SimpleType.HIDDEN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalRelUnion.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelUnion::create ).model( DataModel.RELATIONAL )
                .opName( "REL_UNION" ).opAlias( "UNION" ).numInputs( -1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "all" ).type( ParamType.BOOLEAN ).simpleType( SimpleType.HIDDEN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalRelIntersect.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelIntersect::create ).model( DataModel.RELATIONAL )
                .opName( "REL_INTERSECT" ).opAlias( "INTERSECT" ).numInputs( -1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "all" ).type( ParamType.BOOLEAN ).simpleType( SimpleType.HIDDEN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalModifyCollect.class, PolyAlgDeclaration.builder()
                .creator( LogicalModifyCollect::create ).model( DataModel.RELATIONAL )
                .opName( "REL_MODIFY_COLLECT" ).opAlias( "MODIFY_COLLECT" ).numInputs( -1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "all" ).type( ParamType.BOOLEAN ).simpleType( SimpleType.HIDDEN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalRelSort.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelSort::create ).model( DataModel.RELATIONAL )
                .opName( "REL_SORT" ).opAlias( "SORT" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "sort" ).aliases( List.of( "collation", "order" ) ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "limit" ).alias( "fetch" ).type( ParamType.REX ).simpleType( SimpleType.REX_UINT ).defaultValue( RexArg.NULL ).build() )
                .param( Parameter.builder().name( "offset" ).type( ParamType.REX ).simpleType( SimpleType.HIDDEN ).defaultValue( RexArg.NULL ).build() )
                .build() );
        declarations.put( LogicalRelJoin.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelJoin::create ).model( DataModel.RELATIONAL )
                .opName( "REL_JOIN" ).opAlias( "JOIN" ).numInputs( 2 ).opTags( logAllTags )
                .param( Parameter.builder().name( "condition" ).alias( "on" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .param( Parameter.builder().name( "type" ).type( ParamType.JOIN_TYPE_ENUM ).defaultValue( new EnumArg<>( JoinAlgType.INNER, ParamType.JOIN_TYPE_ENUM ) ).build() )
                .param( Parameter.builder().name( "variables" ).type( ParamType.CORR_ID ).simpleType( SimpleType.HIDDEN ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "semiJoinDone" ).type( ParamType.BOOLEAN ).simpleType( SimpleType.HIDDEN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalCalc.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "REL_CALC" ).opAlias( "CALC" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "exps" ).type( ParamType.REX ).multiValued( 1 ).build() )
                .param( Parameter.builder().name( "projects" ).tag( ParamTag.ALIAS ).type( ParamType.REX ).multiValued( 1 ).build() ) // can have a name
                .param( Parameter.builder().name( "condition" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).defaultValue( RexArg.NULL ).build() )
                .build() );
        declarations.put( LogicalRelModify.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelModify::create ).model( DataModel.RELATIONAL )
                .opName( "REL_MODIFY" ).opAlias( "MODIFY" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "table" ).alias( "target" ).type( ParamType.ENTITY ).build() )
                .param( Parameter.builder().name( "operation" ).type( ParamType.MODIFY_OP_ENUM ).build() )
                .param( Parameter.builder().name( "targets" ).alias( "columns" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "sources" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "flattened" ).type( ParamType.BOOLEAN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalRelValues.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelValues::create ).model( DataModel.RELATIONAL )
                .opName( "REL_VALUES" ).opAlias( "VALUES" ).numInputs( 0 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "names" ).multiValued( 1 ).type( ParamType.STRING ).build() )
                .param( Parameter.builder().name( "tuples" ).multiValued( 2 ).type( ParamType.REX ).build() )
                .build() );
        declarations.put( LogicalRelCorrelate.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelCorrelate::create ).model( DataModel.RELATIONAL )
                .opName( "REL_CORRELATE" ).opAlias( "CORRELATE" ).numInputs( 2 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "id" ).type( ParamType.CORR_ID ).build() )
                .param( Parameter.builder().name( "columns" ).type( ParamType.REX ).build() )
                .param( Parameter.builder().name( "joinType" ).alias( "type" ).type( ParamType.SEMI_JOIN_TYPE_ENUM ).build() )
                .build() );
        declarations.put( LogicalRelExchange.class, PolyAlgDeclaration.builder()
                .creator( LogicalRelExchange::create ).model( DataModel.RELATIONAL )
                .opName( "REL_EXCHANGE" ).opAlias( "EXCHANGE" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "distributionType" ).alias( "type" ).type( ParamType.DISTRIBUTION_TYPE_ENUM ).build() )
                .param( Parameter.builder().name( "numbers" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalSortExchange.class, PolyAlgDeclaration.builder()
                .creator( LogicalSortExchange::create ).model( DataModel.RELATIONAL )
                .opName( "REL_SORT_EXCHANGE" ).opAlias( "SORT_EXCHANGE" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "sort" ).aliases( List.of( "collation", "order" ) ).multiValued( 1 ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).build() )
                .param( Parameter.builder().name( "distributionType" ).alias( "type" ).type( ParamType.DISTRIBUTION_TYPE_ENUM ).build() )
                .param( Parameter.builder().name( "numbers" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        // DOCUMENT
        declarations.put( LogicalDocumentScan.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentScan::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_SCAN" ).numInputs( 0 ).opTags( logAllTags )
                .param( Parameter.builder().name( "entity" ).type( ParamType.ENTITY ).build() )
                .build() );
        declarations.put( LogicalDocumentFilter.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentFilter::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_FILTER" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "condition" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .build() );
        declarations.put( LogicalDocumentSort.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentSort::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_SORT" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "sort" ).aliases( List.of( "collation", "order" ) ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "targets" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "limit" ).alias( "fetch" ).type( ParamType.REX ).simpleType( SimpleType.REX_UINT ).defaultValue( RexArg.NULL ).build() )
                .param( Parameter.builder().name( "offset" ).type( ParamType.REX ).simpleType( SimpleType.HIDDEN ).defaultValue( RexArg.NULL ).build() )
                .build() );
        declarations.put( LogicalDocumentUnwind.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentUnwind::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_UNWIND" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "path" ).type( ParamType.STRING ).build() )
                .build() );
        declarations.put( LogicalDocumentProject.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentProject::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_PROJECT" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "includes" ).tag( ParamTag.ALIAS ).requiresAlias( true ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "excludes" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalDocumentAggregate.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentAggregate::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_AGGREGATE" ).opAlias( "DOC_AGG" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "group" ).type( ParamType.REX ).defaultValue( RexArg.NULL ).build() )
                .param( Parameter.builder().name( "aggregates" ).alias( "aggs" ).multiValued( 1 ).type( ParamType.LAX_AGGREGATE ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalDocumentModify.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentModify::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_MODIFY" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "entity" ).type( ParamType.ENTITY ).build() )
                .param( Parameter.builder().name( "operation" ).type( ParamType.MODIFY_OP_ENUM ).build() )
                .param( Parameter.builder().name( "updates" ).tag( ParamTag.ALIAS ).requiresAlias( true ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "removes" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "renames" ).tag( ParamTag.ALIAS ).requiresAlias( true ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        // GRAPH
        declarations.put( LogicalLpgScan.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgScan::create ).model( DataModel.GRAPH )
                .opName( "LPG_SCAN" ).numInputs( 0 ).opTags( logAllTags )
                .param( Parameter.builder().name( "entity" ).type( ParamType.ENTITY ).build() )
                .build() );
        declarations.put( LogicalLpgMatch.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgMatch::create ).model( DataModel.GRAPH )
                .opName( "LPG_MATCH" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "matches" ).tag( ParamTag.ALIAS ).multiValued( 1 ).type( ParamType.REX ).build() )
                .build() );
        declarations.put( LogicalLpgFilter.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgFilter::create ).model( DataModel.GRAPH )
                .opName( "LPG_FILTER" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "condition" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .build() );
        declarations.put( LogicalLpgProject.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgProject::create ).model( DataModel.GRAPH )
                .opName( "LPG_PROJECT" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "projects" ).tag( ParamTag.ALIAS ).multiValued( 1 ).type( ParamType.REX ).build() )
                .build() );
        declarations.put( LogicalLpgSort.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgSort::create ).model( DataModel.GRAPH )
                .opName( "LPG_SORT" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "sort" ).aliases( List.of( "collation", "order" ) ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "limit" ).alias( "fetch" ).tag( ParamTag.NON_NEGATIVE ).type( ParamType.INTEGER ).defaultValue( IntArg.NULL ).build() )
                .param( Parameter.builder().name( "skip" ).alias( "offset" ).tag( ParamTag.NON_NEGATIVE ).type( ParamType.INTEGER ).simpleType( SimpleType.HIDDEN ).defaultValue( IntArg.NULL ).build() )
                .build() );
        declarations.put( LogicalLpgUnion.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgUnion::create ).model( DataModel.GRAPH )
                .opName( "LPG_UNION" ).numInputs( -1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "all" ).type( ParamType.BOOLEAN ).simpleType( SimpleType.HIDDEN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( LogicalLpgUnwind.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgUnwind::create ).model( DataModel.GRAPH )
                .opName( "LPG_UNWIND" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "index" ).tag( ParamTag.NON_NEGATIVE ).type( ParamType.INTEGER ).build() )
                .param( Parameter.builder().name( "alias" ).type( ParamType.STRING ).defaultValue( StringArg.NULL ).build() )
                .build() );
        declarations.put( LogicalLpgAggregate.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgAggregate::create ).model( DataModel.GRAPH )
                .opName( "LPG_AGGREGATE" ).opAlias( "LPG_AGG" ).numInputs( 1 ).opTags( logAllTags )
                .param( Parameter.builder().name( "groups" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "aggregates" ).alias( "aggs" ).multiValued( 1 ).type( ParamType.LAX_AGGREGATE ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalLpgModify.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgModify::create ).model( DataModel.GRAPH )
                .opName( "LPG_MODIFY" ).numInputs( 1 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "entity" ).type( ParamType.ENTITY ).build() )
                .param( Parameter.builder().name( "operation" ).type( ParamType.MODIFY_OP_ENUM ).build() )
                .param( Parameter.builder().name( "updates" ).alias( "operations" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "ids" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        // Common
        declarations.put( LogicalBatchIterator.class, PolyAlgDeclaration.builder()
                .creator( LogicalBatchIterator::create ).model( null )
                .opName( "BATCH_ITERATOR" ).opAlias( "BATCH" ).numInputs( 1 ).opTags( logAllProTags )
                .build() );
    }


    private static void populateClassesMap() {
        for ( Map.Entry<Class<? extends AlgNode>, PolyAlgDeclaration> entry : declarations.entrySet() ) {
            Class<? extends AlgNode> clazz = entry.getKey();
            PolyAlgDeclaration declaration = entry.getValue();

            assert !classes.containsKey( declaration.opName );
            classes.put( declaration.opName, clazz );
            for ( String alias : declaration.opAliases ) {
                assert !classes.containsKey( alias );
                classes.put( alias, clazz );
            }
        }
    }


    public static PolyAlgDeclaration getDeclaration( Class<? extends AlgNode> clazz ) {
        return getDeclaration( clazz, DataModel.getDefault(), 0 );
    }


    /**
     * Retrieves the PolyAlgDeclaration associated with the specified class from a map of declarations,
     * or returns a default PolyAlgDeclaration if none is found.
     *
     * @param clazz The class for which the PolyAlgDeclaration is being retrieved
     * @param model the default DataModel to be used if the class is not registered
     * @param numInputs The number of inputs associated with the PolyAlgDeclaration if a new one is created.
     * @return The PolyAlgDeclaration associated with the specified class if found in the map,
     * or a new PolyAlgDeclaration initialized with the class name and the specified number of inputs.
     */
    public static PolyAlgDeclaration getDeclaration( Class<? extends AlgNode> clazz, DataModel model, int numInputs ) {
        return declarations.getOrDefault(
                clazz,
                PolyAlgDeclaration.builder().opName( clazz.getSimpleName() ).model( model ).numInputs( numInputs ).build() );
    }


    public static Class<? extends AlgNode> getClass( String opName ) {
        return classes.get( opName );
    }


    /**
     * Retrieves the PolyAlgDeclaration associated with the specified operator
     * or returns null if the operator is not known.
     * It is also possible to use an alias for the operator name.
     *
     * @param opName The name of the operator or one of its aliases
     * @return The PolyAlgDeclaration associated with the opName if it exists,
     * or null otherwise.
     */
    public static PolyAlgDeclaration getDeclaration( String opName ) {
        return declarations.get( getClass( opName ) );
    }


    public static ObjectNode serialize() {
        if ( serialized == null ) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();

            ObjectNode decls = mapper.createObjectNode();

            for ( PolyAlgDeclaration decl : declarations.values() ) {
                decls.set( decl.opName, decl.serialize( mapper ) );
            }
            node.set( "declarations", decls );

            ObjectNode enums = mapper.createObjectNode();
            for ( ParamType type : ParamType.getEnumParamTypes() ) {
                ArrayNode values = mapper.createArrayNode();
                for ( Enum<?> enumValue : type.getEnumClass().getEnumConstants() ) {
                    values.add( enumValue.name() );
                }
                enums.set( type.name(), values );
            }

            ArrayNode values = mapper.createArrayNode();
            for ( OperatorName operator : OperatorName.values() ) {
                if ( operator.getClazz() == AggFunction.class ) {
                    values.add( operator.name() );
                }
            }
            enums.set( "AggFunctionOperator", values );

            node.set( "enums", enums );

            serialized = node;
        }

        return serialized;
    }

}
