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

import java.util.Collections;
import java.util.Objects;
import org.polypheny.db.mql.Mql;
import org.polypheny.db.mql.MqlFind;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.sql.SqlKind;

public class MqlToRelConverter {

    private PolyphenyDbCatalogReader catalogReader;
    private final RelOptCluster cluster;


    public MqlToRelConverter( MqlProcessor mqlProcessor, PolyphenyDbCatalogReader catalogReader, RelOptCluster cluster ) {
        this.catalogReader = catalogReader;
        this.cluster = Objects.requireNonNull( cluster );

    }


    public RelRoot convertQuery( MqlNode query, boolean b, boolean b1 ) {
        Mql.Type kind = query.getKind();
        switch ( kind ) {
            case FIND:
                return RelRoot.of( convertFind( (MqlFind) query ), SqlKind.SELECT );
            default:
                throw new IllegalStateException( "Unexpected value: " + kind );
        }

    }


    private RelNode convertFind( MqlFind query ) {
        //RelOptTable table = SqlValidatorUtil.getRelOptTable( null, null, null, null );
        return LogicalTableScan.create( cluster, catalogReader.getTable( Collections.singletonList( query.getCollection() ) ) );
    }

}
