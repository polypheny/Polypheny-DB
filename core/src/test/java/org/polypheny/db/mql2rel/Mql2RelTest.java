/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.mql2rel;

import org.polypheny.db.mql.MqlTest;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.test.MockRelOptPlanner;
import org.polypheny.db.test.catalog.MockCatalogReader;
import org.polypheny.db.test.catalog.MockCatalogReaderDocument;
import org.polypheny.db.type.PolyTypeFactoryImpl;


public abstract class Mql2RelTest extends MqlTest {

    private static final RelDataTypeFactory factory;
    private static final RelOptCluster cluster;
    final static MqlToRelConverter mqlToRelConverter;

    private static final MockCatalogReader reader;


    static {
        factory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        cluster = RelOptCluster.create( new MockRelOptPlanner( Contexts.empty() ), new RexBuilder( factory ) );
        reader = new MockCatalogReaderDocument( factory, false );
        reader.init();
        mqlToRelConverter = new MqlToRelConverter( null, reader, cluster );
    }


    public RelRoot translate( String mql ) {
        return mqlToRelConverter.convert( parse( mql ), "private" );
    }

}
