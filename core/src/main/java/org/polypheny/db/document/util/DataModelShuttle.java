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

package org.polypheny.db.document.util;

import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalDocuments;
import org.polypheny.db.rel.logical.LogicalValues;

/**
 * Shuttle, which transforms a normal LogicalValues to LogicalValues
 */
public class DataModelShuttle extends RelShuttleImpl {

    @Override
    public RelNode visit( LogicalValues values ) {
        return super.visit( values );
    }


    @Override
    public RelNode visit( RelNode other ) {
        if ( other instanceof TableModify ) {
            TableModify modify = (TableModify) other;
            if ( modify.getTable().getTable().getSchemaType() == SchemaType.DOCUMENT ) {
                if ( modify.getInput() instanceof LogicalValues && !(modify.getInput() instanceof LogicalDocuments) ) {
                    modify.replaceInput( 0, LogicalDocuments.create( (LogicalValues) modify.getInput() ) );
                }

                return super.visit( other );
            }
            return super.visit( other );
        }
        return super.visit( other );
    }

}
