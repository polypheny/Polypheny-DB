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


import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.validate.ValidatorCatalogReader;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.util.Moniker;


/**
 * Implementation of {@link ValidatorCatalogReader} that passes all calls to a parent catalog reader.
 */
public abstract class DelegatingSqlValidatorCatalogReader implements ValidatorCatalogReader {

    protected final ValidatorCatalogReader catalogReader;


    /**
     * Creates a DelegatingSqlValidatorCatalogReader.
     *
     * @param catalogReader Parent catalog reader
     */
    public DelegatingSqlValidatorCatalogReader( ValidatorCatalogReader catalogReader ) {
        this.catalogReader = catalogReader;
    }


    @Override
    public ValidatorTable getTable( List<String> names ) {
        return catalogReader.getTable( names );
    }


    @Override
    public AlgDataType getNamedType( Identifier typeName ) {
        return catalogReader.getNamedType( typeName );
    }


    @Override
    public List<Moniker> getAllSchemaObjectNames( List<String> names ) {
        return catalogReader.getAllSchemaObjectNames( names );
    }


    @Override
    public List<List<String>> getSchemaPaths() {
        return catalogReader.getSchemaPaths();
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        return catalogReader.unwrap( aClass );
    }

}

