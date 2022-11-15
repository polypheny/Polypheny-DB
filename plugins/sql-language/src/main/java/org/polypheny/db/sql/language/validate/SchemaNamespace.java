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

package org.polypheny.db.sql.language.validate;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.Util;


/**
 * Namespace based on a schema.
 *
 * The visible names are tables and sub-schemas.
 */
class SchemaNamespace extends AbstractNamespace {

    /**
     * The path of this schema.
     */
    private final ImmutableList<String> names;


    /**
     * Creates a SchemaNamespace.
     */
    SchemaNamespace( SqlValidatorImpl validator, ImmutableList<String> names ) {
        super( validator, null );
        this.names = Objects.requireNonNull( names );
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        final AlgDataTypeFactory.Builder builder = validator.getTypeFactory().builder();
        for ( Moniker moniker : validator.catalogReader.getAllSchemaObjectNames( names ) ) {
            final List<String> names1 = moniker.getFullyQualifiedNames();
            final ValidatorTable table = validator.catalogReader.getTable( names1 );
            builder.add( Util.last( names1 ), null, table.getRowType() );
        }
        return builder.build();
    }


    @Override
    public SqlNode getNode() {
        return null;
    }

}
