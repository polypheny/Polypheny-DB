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

package org.polypheny.db.mql.mql2alg;

import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.MockCatalogReader;
import org.polypheny.db.catalog.MockCatalogReaderDocument;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.mql.mql.MqlTest;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.test.MockRelOptPlanner;
import org.polypheny.db.type.PolyTypeFactoryImpl;


public abstract class Mql2AlgTest extends MqlTest {

    private static final AlgDataTypeFactory factory;
    private static final AlgOptCluster cluster;
    final static MqlToAlgConverter MQL_TO_ALG_CONVERTER;

    private static final MockCatalogReader reader;


    static {
        factory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        cluster = AlgOptCluster.create( new MockRelOptPlanner( Contexts.empty() ), new RexBuilder( factory ) );
        reader = new MockCatalogReaderDocument( factory, false );
        reader.init();
        MQL_TO_ALG_CONVERTER = new MqlToAlgConverter( null, reader, cluster );
    }


    public AlgRoot translate( String mql ) {
        return MQL_TO_ALG_CONVERTER.convert( parse( mql ), new MqlQueryParameters( mql, "private", NamespaceType.DOCUMENT ) );
    }

}
