/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet;

import com.google.common.collect.ImmutableList;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.parquet.document.ParquetDocumentSource;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocScan;
import org.polypheny.db.adapter.parquet.relational.ParquetRelationalSource;
import org.polypheny.db.adapter.parquet.relational.planning.EnumerableParquet;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetEnumerableUnion;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetConvention;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelAggregate;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelJoin;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelMetadataScan;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.algebra.enumerable.EnumerableUnion;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.OperatorTag;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;

/**
 * Plugin entry point for the Parquet adapter.
 */
@SuppressWarnings("unused")
public class ParquetPlugin extends PolyPlugin {

    private long relationalId;
    private long documentId;

    /**
     * Constructor
     * Create Plugin Instance
     */
    @SuppressWarnings("unused")
    public ParquetPlugin( PluginContext context ) {
        super( context );
        registerPolyAlg();
    }


    /**
     * Register nodes
     */
    private void registerPolyAlg() {
        if ( PolyAlgRegistry.getClass( "P_SCAN" ) != null ) {
            return;
        }

        ImmutableList<OperatorTag> physTags = ImmutableList.of( OperatorTag.PHYSICAL, OperatorTag.ADVANCED );

        PolyAlgRegistry.register( EnumerableParquet.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "PE_CALC" ).convention( EnumerableConvention.INSTANCE ).numInputs( 1 ).opTags( physTags )
                .build() );

        PolyAlgRegistry.register( ParquetEnumerableUnion.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "PE_UNION" ).convention( EnumerableConvention.INSTANCE ).numInputs( -1 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( EnumerableUnion.class ) )
                .build() );

        PolyAlgRegistry.register( ParquetRelScan.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "P_SCAN" ).convention( ParquetConvention.INSTANCE ).numInputs( 0 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelScan.class ) )
                .param( Parameter.builder().name( "fields" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "filters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        PolyAlgRegistry.register( ParquetRelMetadataScan.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "P_METADATA_SCAN" ).convention( ParquetConvention.INSTANCE ).numInputs( 0 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalRelScan.class ) )
                .param( Parameter.builder().name( "fields" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "filters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        PolyAlgRegistry.register( ParquetRelJoin.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "P_JOIN" ).convention( ParquetConvention.INSTANCE ).numInputs( 2 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( EnumerableJoin.class ) )
                .param( Parameter.builder().name( "leftIsParent" ).type( ParamType.BOOLEAN ).defaultValue( BooleanArg.FALSE ).build() )
                .param( Parameter.builder().name( "leftFields" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "rightFields" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "leftFilters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "rightFilters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "joinFilters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        PolyAlgRegistry.register( ParquetRelAggregate.class, PolyAlgDeclaration.builder()
                .model( DataModel.RELATIONAL )
                .opName( "P_AGGREGATE" ).convention( ParquetConvention.INSTANCE ).numInputs( 1 ).opTags( physTags )
                .param( Parameter.builder().name( "mode" ).type( ParamType.STRING ).build() )
                .param( Parameter.builder().name( "fields" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "groups" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "condition" ).type( ParamType.STRING ).build() )
                .param( Parameter.builder().name( "aggregates" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "filters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );

        PolyAlgRegistry.register( ParquetDocScan.class, PolyAlgDeclaration.builder()
                .model( DataModel.DOCUMENT )
                .opName( "P_DOC_SCAN" ).convention( EnumerableConvention.INSTANCE ).numInputs( 0 ).opTags( physTags )
                .params( PolyAlgRegistry.getParams( LogicalDocumentScan.class ) )
                .param( Parameter.builder().name( "fields" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .param( Parameter.builder().name( "filters" ).multiValued( 1 ).type( ParamType.STRING ).defaultValue( ListArg.EMPTY ).build() )
                .build() );
    }


    /**
     * Registers the adapter template once the catalog is ready.
     */
    @Override
    public void afterCatalogInit() {
        this.relationalId = AdapterManager.addAdapterTemplate( ParquetRelationalSource.class, ParquetRelationalSource.NAME, ParquetRelationalSource::new );
        this.documentId = AdapterManager.addAdapterTemplate( ParquetDocumentSource.class, ParquetDocumentSource.NAME, ParquetDocumentSource::new );
    }

    /**
     * Removes the adapter template on shutdown.
     */
    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( relationalId );
        AdapterManager.removeAdapterTemplate( documentId );
    }

}
