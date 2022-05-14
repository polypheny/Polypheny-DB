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

package org.polypheny.db.schema.document;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.logical.document.LogicalDocumentsValues;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;


/**
 * Shuttle, which transforms a normal LogicalValues to LogicalValues
 */
public class DataModelShuttle extends AlgShuttleImpl {

    private final AlgBuilder builder;


    public DataModelShuttle( AlgBuilder builder ) {
        this.builder = builder;
    }


    @Override
    public AlgNode visit( LogicalValues values ) {
        return super.visit( values );
    }


    @Override
    public AlgNode visit( LogicalModify modify ) {
        if ( modify.getTable().getTable().getSchemaType() == NamespaceType.DOCUMENT ) {
            if ( modify.getInput() instanceof LogicalValues && !(modify.getInput() instanceof LogicalDocumentsValues) ) {
                modify.replaceInput( 0, LogicalDocumentsValues.create( (LogicalValues) modify.getInput() ) );
            }
            return super.visit( modify );
        }
        return super.visit( modify );
    }


    private static boolean containsType( AlgDataType rowType, PolyType type ) {
        return rowType.getFieldList().stream().anyMatch( f -> f.getType().getPolyType() == type );
    }


}
