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

package org.polypheny.db.algebra.polyalg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableInterpreter;
import org.polypheny.db.algebra.enumerable.EnumerableIntersect;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.enumerable.EnumerableMergeJoin;
import org.polypheny.db.algebra.enumerable.EnumerableMinus;
import org.polypheny.db.algebra.enumerable.EnumerableProject;
import org.polypheny.db.algebra.enumerable.EnumerableSemiJoin;
import org.polypheny.db.algebra.enumerable.EnumerableSort;
import org.polypheny.db.algebra.enumerable.EnumerableTransformer;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.algebra.enumerable.EnumerableValues;
import org.polypheny.db.algebra.enumerable.common.EnumerableCollect;
import org.polypheny.db.algebra.enumerable.common.EnumerableContextSwitcher;
import org.polypheny.db.algebra.enumerable.common.EnumerableModifyCollect;
import org.polypheny.db.algebra.enumerable.lpg.EnumerableLpgMatch;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.common.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentUnwind;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
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
import org.polypheny.db.algebra.logical.relational.LogicalWindow;
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
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Bindables.BindableScan;
import org.polypheny.db.plan.Convention;

@Slf4j
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
                .param( Parameter.builder().name( "order" ).aliases( List.of( "collation", "key", "sort" ) ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
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
                .param( Parameter.builder().name( "exprs" ).type( ParamType.REX ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "projects" ).tag( ParamTag.ALIAS ).type( ParamType.REX ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "condition" ).type( ParamType.REX ).defaultValue( RexArg.NULL ).build() )
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
                .param( Parameter.builder().name( "order" ).aliases( List.of( "collation", "sort", "key" ) ).multiValued( 1 ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).build() )
                .param( Parameter.builder().name( "distributionType" ).alias( "type" ).type( ParamType.DISTRIBUTION_TYPE_ENUM ).build() )
                .param( Parameter.builder().name( "numbers" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalWindow.class, PolyAlgDeclaration.builder()
                .creator( LogicalWindow::create ).model( DataModel.RELATIONAL )
                .opName( "REL_WINDOW" ).opAlias( "WINDOW" ).numInputs( 1 ).opTags( logAllProTags ).isNotFullyImplemented( true )
                .param( Parameter.builder().name( "constants" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "groups" ).multiValued( 1 ).type( ParamType.WINDOW_GROUP ).defaultValue( ListArg.EMPTY ).build() )
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
                .param( Parameter.builder().name( "order" ).aliases( List.of( "collation", "key", "sort" ) ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
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
        declarations.put( LogicalDocumentValues.class, PolyAlgDeclaration.builder()
                .creator( LogicalDocumentValues::create ).model( DataModel.DOCUMENT )
                .opName( "DOC_VALUES" ).numInputs( 0 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "docs" ).alias( "documents" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "dynamic" ).multiValued( 1 ).type( ParamType.REX ).simpleType( SimpleType.HIDDEN ).defaultValue( ListArg.EMPTY ).build() )
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
                .param( Parameter.builder().name( "order" ).aliases( List.of( "collation", "key", "sort" ) ).type( ParamType.COLLATION ).simpleType( SimpleType.SIMPLE_COLLATION ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
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
                .param( Parameter.builder().name( "groups" ).alias( "group" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
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
        declarations.put( LogicalLpgTransformer.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgTransformer::create ).model( DataModel.GRAPH )
                .opName( "LPG_TRANSFORMER" ).numInputs( -1 ).opTags( ImmutableList.of( OperatorTag.ALLOCATION, OperatorTag.ADVANCED ) )
                .param( Parameter.builder().name( "operation" ).type( ParamType.MODIFY_OP_ENUM ).build() )
                .param( Parameter.builder().name( "order" ).multiValued( 1 ).type( ParamType.POLY_TYPE_ENUM ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( LogicalLpgValues.class, PolyAlgDeclaration.builder()
                .creator( LogicalLpgValues::create ).model( DataModel.GRAPH )
                .opName( "LPG_VALUES" ).numInputs( 0 ).opTags( logAllProTags )
                .param( Parameter.builder().name( "nodes" ).multiValued( 1 ).type( ParamType.REX ).tag( ParamTag.POLY_NODE ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "edges" ).multiValued( 1 ).type( ParamType.REX ).tag( ParamTag.POLY_PATH ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "values" ).multiValued( 2 ).type( ParamType.REX ).defaultValue( ListArg.NESTED_EMPTY ).build() )
                .build() );

        // Common
        declarations.put( LogicalBatchIterator.class, PolyAlgDeclaration.builder()
                .creator( LogicalBatchIterator::create ).model( null )
                .opName( "BATCH_ITERATOR" ).opAlias( "BATCH" ).numInputs( 1 ).opTags( logAllProTags )
                .build() );
        declarations.put( LogicalTransformer.class, PolyAlgDeclaration.builder()
                .creator( LogicalTransformer::create ).model( null )
                .opName( "TRANSFORMER" ).numInputs( -1 ).opTags( ImmutableList.of( OperatorTag.ALLOCATION, OperatorTag.ADVANCED ) )
                .param( Parameter.builder().name( "out" ).alias( "outModel" ).type( ParamType.DATAMODEL_ENUM ).build() )
                .param( Parameter.builder().name( "names" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        // Physical
        addEnumerableDeclarations();
        addBindableDeclarations();
    }


    private static void addEnumerableDeclarations() {
        ImmutableList<OperatorTag> physTags = ImmutableList.of( OperatorTag.PHYSICAL, OperatorTag.ADVANCED );
        Convention c = EnumerableConvention.INSTANCE;

        declarations.put( EnumerableProject.class, PolyAlgDeclaration.builder()
                .creator( EnumerableProject::create ).model( null )
                .opName( "E_PROJECT" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( getParams( LogicalRelProject.class ) )
                .build() );
        declarations.put( EnumerableInterpreter.class, PolyAlgDeclaration.builder()
                .creator( EnumerableInterpreter::create ).model( null )
                .opName( "E_INTERPRETER" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .param( Parameter.builder().name( "factor" ).tag( ParamTag.NON_NEGATIVE ).type( ParamType.DOUBLE ).build() )
                .build() );
        declarations.put( EnumerableAggregate.class, PolyAlgDeclaration.builder()
                .creator( EnumerableAggregate::create ).model( null )
                .opName( "E_AGGREGATE" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( getParams( LogicalRelAggregate.class ) )
                .build() );
        declarations.put( EnumerableCalc.class, PolyAlgDeclaration.builder()
                .creator( EnumerableCalc::create ).model( null )
                .opName( "E_CALC" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( getParams( LogicalCalc.class ) )
                .build() );
        declarations.put( EnumerableJoin.class, PolyAlgDeclaration.builder()
                .creator( EnumerableJoin::create ).model( null )
                .opName( "E_JOIN" ).numInputs( 2 ).opTags( physTags )
                .param( Parameter.builder().name( "condition" ).alias( "on" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .param( Parameter.builder().name( "type" ).type( ParamType.JOIN_TYPE_ENUM ).defaultValue( new EnumArg<>( JoinAlgType.INNER, ParamType.JOIN_TYPE_ENUM ) ).build() )
                .param( Parameter.builder().name( "variables" ).type( ParamType.CORR_ID ).simpleType( SimpleType.HIDDEN ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "leftKeys" ).multiValued( 1 ).type( ParamType.INTEGER ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "rightKeys" ).multiValued( 1 ).type( ParamType.INTEGER ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( EnumerableMergeJoin.class, PolyAlgDeclaration.builder()
                .creator( EnumerableMergeJoin::create ).model( null )
                .opName( "E_MERGE_JOIN" ).numInputs( 2 ).opTags( physTags )
                .params( getParams( EnumerableJoin.class ) )
                .build() );
        declarations.put( EnumerableSemiJoin.class, PolyAlgDeclaration.builder()
                .creator( EnumerableSemiJoin::create ).model( null )
                .opName( "E_SEMI_JOIN" ).numInputs( 2 ).opTags( physTags )
                .param( Parameter.builder().name( "condition" ).alias( "on" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .param( Parameter.builder().name( "leftKeys" ).multiValued( 1 ).type( ParamType.INTEGER ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "rightKeys" ).multiValued( 1 ).type( ParamType.INTEGER ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        declarations.put( EnumerableSort.class, PolyAlgDeclaration.builder()
                .creator( EnumerableSort::create ).model( null )
                .opName( "E_SORT" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( getParams( LogicalRelSort.class ) )
                .build() );
        declarations.put( EnumerableUnion.class, PolyAlgDeclaration.builder()
                .creator( EnumerableUnion::create ).model( null )
                .opName( "E_UNION" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( getParams( LogicalRelUnion.class ) )
                .build() );
        declarations.put( EnumerableIntersect.class, PolyAlgDeclaration.builder()
                .creator( EnumerableIntersect::create ).model( null )
                .opName( "E_INTERSECT" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( getParams( LogicalRelIntersect.class ) )
                .build() );
        declarations.put( EnumerableMinus.class, PolyAlgDeclaration.builder()
                .creator( EnumerableMinus::create ).model( null )
                .opName( "E_MINUS" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( getParams( LogicalRelMinus.class ) )
                .build() );
        declarations.put( EnumerableValues.class, PolyAlgDeclaration.builder()
                .creator( EnumerableValues::create ).model( DataModel.RELATIONAL )
                .opName( "E_VALUES" ).convention( c ).numInputs( 0 ).opTags( physTags )
                .params( getParams( LogicalRelValues.class ) )
                .build() );
        declarations.put( EnumerableLimit.class, PolyAlgDeclaration.builder()
                .creator( EnumerableLimit::create ).model( null )
                .opName( "E_LIMIT" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .param( Parameter.builder().name( "limit" ).alias( "fetch" ).type( ParamType.REX ).simpleType( SimpleType.REX_UINT ).defaultValue( RexArg.NULL ).build() )
                .param( Parameter.builder().name( "offset" ).type( ParamType.REX ).simpleType( SimpleType.HIDDEN ).defaultValue( RexArg.NULL ).build() )
                .build() );
        declarations.put( EnumerableTransformer.class, PolyAlgDeclaration.builder()
                .creator( EnumerableTransformer::create ).model( null )
                .opName( "E_TRANSFORMER" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .param( Parameter.builder().name( "out" ).alias( "outModel" ).type( ParamType.DATAMODEL_ENUM ).build() )
                .param( Parameter.builder().name( "names" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "isCrossModel" ).type( ParamType.BOOLEAN ).defaultValue( BooleanArg.FALSE ).build() )
                .build() );
        declarations.put( EnumerableLpgMatch.class, PolyAlgDeclaration.builder()
                .creator( EnumerableLpgMatch::create ).model( DataModel.GRAPH )
                .opName( "E_LPG_MATCH" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( getParams( LogicalLpgMatch.class ) )
                .build() );
        declarations.put( EnumerableCollect.class, PolyAlgDeclaration.builder()
                .creator( EnumerableCollect::create ).model( null )
                .opName( "E_COLLECT" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .param( Parameter.builder().name( "field" ).type( ParamType.STRING ).build() )
                .build() );
        declarations.put( EnumerableModifyCollect.class, PolyAlgDeclaration.builder()
                .creator( EnumerableModifyCollect::create ).model( null )
                .opName( "E_MODIFY_COLLECT" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( getParams( LogicalModifyCollect.class ) )
                .build() );
        declarations.put( EnumerableContextSwitcher.class, PolyAlgDeclaration.builder()
                .creator( EnumerableContextSwitcher::create ).model( null )
                .opName( "E_CONTEXT_SWITCHER" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .build() );
    }


    private static void addBindableDeclarations() {
        ImmutableList<OperatorTag> physTags = ImmutableList.of( OperatorTag.PHYSICAL, OperatorTag.ADVANCED );
        Convention c = BindableConvention.INSTANCE;

        declarations.put( BindableScan.class, PolyAlgDeclaration.builder()
                .creator( BindableScan::create ).model( DataModel.RELATIONAL )
                .opName( "BINDABLE_SCAN" ).convention( c ).numInputs( 0 ).opTags( physTags )
                .param( Parameter.builder().name( "entity" ).type( ParamType.ENTITY ).build() )
                .param( Parameter.builder().name( "filters" ).multiValued( 1 ).type( ParamType.REX ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "projects" ).multiValued( 1 ).type( ParamType.INTEGER ).defaultValue( ListArg.EMPTY ).build() )
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


    /**
     * Registers the specified declaration for the given AlgNode class.
     * This is only allowed during the initialization of Polypheny-DB.
     * As soon as a UI instance has requested the registry, this will result in an assertion error.
     * <p>
     * General steps to follow for adding support for a new AlgNode:
     * <ol>
     *     <li>Create a {@link PolyAlgDeclaration} that declares the parameters and their corresponding types.
     *     If no fitting {@link ParamType} exists yet, you can create a new one together with a corresponding {@link org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg}</li>
     *     <li>Implement {@link AlgNode#bindArguments()} or use the implementation of a superclass
     *     (given that it is compatible with the declaration). </li>
     *     <li>Write a static creator method in your AlgNode that has the signature {@code create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster )} </li>
     *     <li>Register the declaration once during initialization using this method.</li>
     * </ol>
     *
     * @param clazz The class for which the PolyAlgDeclaration is being registered
     * @param decl The PolyAlgDeclaration to register
     */
    public static void register( Class<? extends AlgNode> clazz, PolyAlgDeclaration decl ) {
        if ( serialized != null ) {
            log.warn( "PolyAlg operator was registered after the registry was already serialized!" );
        }
        assert !declarations.containsKey( clazz );
        declarations.put( clazz, decl );

        assert !classes.containsKey( decl.opName );
        classes.put( decl.opName, clazz );
        for ( String alias : decl.opAliases ) {
            assert !classes.containsKey( alias );
            classes.put( alias, clazz );
        }
    }


    /**
     * Retrieves a mutable list containing all parameters of a previously registered declaration.
     * This can be useful when multiple operators share the same arguments.
     *
     * @param clazz the class whose declaration will be used
     * @return a list containing all parameters of the declaration corresponding to the specified class
     */
    public static List<Parameter> getParams( Class<? extends AlgNode> clazz ) {
        PolyAlgDeclaration decl = declarations.get( clazz );
        List<Parameter> params = new ArrayList<>( decl.posParams );
        params.addAll( decl.kwParams );
        return params;
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
