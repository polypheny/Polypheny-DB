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

package org.polypheny.db.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcAggregate;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcCalc;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcFilter;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcIntersect;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcJoin;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcMinus;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcProject;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcSort;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcTableModify;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcUnion;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcValues;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.OperatorTag;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.SimpleType;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;

public class JdbcAdapterFramework extends PolyPlugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public JdbcAdapterFramework( PluginContext context ) {
        super( context );
        registerPolyAlg();
    }


    private void registerPolyAlg() {
        ImmutableList<OperatorTag> physTags = ImmutableList.of( OperatorTag.PHYSICAL, OperatorTag.ADVANCED );
        Convention c = JdbcConvention.NONE; // TODO: set correct convention

        PolyAlgRegistry.register( JdbcProject.class, PolyAlgDeclaration.builder()
                .creator( JdbcProject::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_PROJECT" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelProject.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcToEnumerableConverter.class, PolyAlgDeclaration.builder()
                .creator( JdbcToEnumerableConverter::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_TO_E" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .build() );
        PolyAlgRegistry.register( JdbcJoin.class, PolyAlgDeclaration.builder()
                .creator( JdbcJoin::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_JOIN" ).convention( c ).numInputs( 2 ).opTags( physTags )
                .param( Parameter.builder().name( "condition" ).alias( "on" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .param( Parameter.builder().name( "type" ).type( ParamType.JOIN_TYPE_ENUM ).defaultValue( new EnumArg<>( JoinAlgType.INNER, ParamType.JOIN_TYPE_ENUM ) ).build() )
                .param( Parameter.builder().name( "variables" ).type( ParamType.CORR_ID ).simpleType( SimpleType.HIDDEN ).multiValued( 1 ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
        PolyAlgRegistry.register( JdbcCalc.class, PolyAlgDeclaration.builder()
                .creator( JdbcCalc::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_CALC" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalCalc.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcFilter.class, PolyAlgDeclaration.builder()
                .creator( JdbcFilter::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_FILTER" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .param( Parameter.builder().name( "condition" ).type( ParamType.REX ).simpleType( SimpleType.REX_PREDICATE ).build() )
                .build() );
        PolyAlgRegistry.register( JdbcAggregate.class, PolyAlgDeclaration.builder()
                .creator( JdbcAggregate::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_AGGREGATE" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelAggregate.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcSort.class, PolyAlgDeclaration.builder()
                .creator( JdbcSort::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_SORT" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelSort.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcUnion.class, PolyAlgDeclaration.builder()
                .creator( JdbcUnion::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_UNION" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelUnion.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcIntersect.class, PolyAlgDeclaration.builder()
                .creator( JdbcIntersect::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_INTERSECT" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelIntersect.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcMinus.class, PolyAlgDeclaration.builder()
                .creator( JdbcMinus::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_MINUS" ).convention( c ).numInputs( -1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelMinus.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcTableModify.class, PolyAlgDeclaration.builder()
                .creator( JdbcTableModify::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_MODIFY" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelModify.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcValues.class, PolyAlgDeclaration.builder()
                .creator( JdbcValues::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_VALUES" ).convention( c ).numInputs( 0 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelValues.class ) )
                .build() );
        PolyAlgRegistry.register( JdbcScan.class, PolyAlgDeclaration.builder()
                .creator( JdbcScan::create ).model( DataModel.RELATIONAL )
                .opName( "JDBC_SCAN" ).convention( c ).numInputs( 0 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelScan.class ) )
                .build() );

    }


}
