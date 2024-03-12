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

package org.polypheny.db.mql.mql2alg;

import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.mql.mql.MqlTest;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;


public abstract class Mql2AlgTest extends MqlTest {

    private static final AlgDataTypeFactory factory;
    private static final AlgCluster cluster;
    final static MqlToAlgConverter MQL_TO_ALG_CONVERTER;

    private static final Snapshot snapshot;


    static {
        factory = AlgDataTypeFactory.DEFAULT;
        cluster = AlgCluster.create( new VolcanoPlanner(), new RexBuilder( factory ), null, null );
        snapshot = Catalog.snapshot();
        MQL_TO_ALG_CONVERTER = new MqlToAlgConverter( snapshot, cluster );
    }


    public AlgRoot translate( String mql ) {
        return MQL_TO_ALG_CONVERTER.convert(
                ParsedQueryContext.builder()
                        .query( mql )
                        .queryNode( parse( mql ) )
                        .language( QueryLanguage.from( "mql" ) )
                        .origin( "Mql Test" ).build() );
    }

}
