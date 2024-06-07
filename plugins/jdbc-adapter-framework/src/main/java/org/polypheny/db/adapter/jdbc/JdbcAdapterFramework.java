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

package org.polypheny.db.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.polypheny.db.adapter.jdbc.JdbcRules.JdbcProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.OperatorTag;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
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
                .opName( "JDBC_TO_ENUMERABLE" ).convention( c ).numInputs( 1 ).opTags( physTags )
                .build() );
    }


}
