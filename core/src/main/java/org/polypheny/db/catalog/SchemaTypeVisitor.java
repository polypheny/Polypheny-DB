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

package org.polypheny.db.catalog;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.Table;

public class SchemaTypeVisitor extends AlgShuttleImpl {

    private final List<NamespaceType> namespaceTypes = new ArrayList<>();


    public NamespaceType getSchemaTypes() {
        if ( namespaceTypes.stream().allMatch( s -> s == NamespaceType.RELATIONAL ) ) {
            return NamespaceType.RELATIONAL;
        } else if ( namespaceTypes.stream().allMatch( s -> s == NamespaceType.DOCUMENT ) ) {
            return NamespaceType.DOCUMENT;
        } else {
            //mixed
            return null;
        }
    }


    @Override
    public AlgNode visit( Scan scan ) {
        try {
            List<String> names = scan.getTable().getQualifiedName();
            CatalogSchema schema;
            if ( names.size() == 3 ) {
                schema = Catalog.getInstance().getSchema( names.get( 0 ), names.get( 1 ) );
            } else if ( names.size() == 2 ) {
                if ( names.get( 0 ).contains( "_" ) ) {
                    schema = Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, names.get( 0 ).split( "_" )[names.size() - 1] );
                } else {
                    schema = Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, names.get( 0 ) );
                }
            } else {
                Table logicalTable = scan.getTable().getTable();
                if ( logicalTable instanceof LogicalTable ) {
                    schema = Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, ((LogicalTable) logicalTable).getLogicalSchemaName() );
                } else {
                    throw new RuntimeException( "The used table did not use a full name." );
                }
            }
            namespaceTypes.add( schema.getNamespaceType() );
        } catch ( UnknownSchemaException | UnknownDatabaseException e ) {
            throw new RuntimeException( "The was an error on retrieval of the data model." );
        }
        return super.visit( scan );
    }

}
