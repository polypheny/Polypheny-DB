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

package org.polypheny.db.languages.sql.validate;


import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.core.util.Conformance;
import org.polypheny.db.core.operators.OperatorTable;
import org.polypheny.db.languages.sql.SqlInsert;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rel.type.RelDataType;


/**
 * Validator.
 */
public class PolyphenyDbSqlValidator extends SqlValidatorImpl {

    public PolyphenyDbSqlValidator(
            OperatorTable opTab,
            PolyphenyDbCatalogReader catalogReader,
            JavaTypeFactory typeFactory,
            Conformance conformance ) {
        super( opTab, catalogReader, typeFactory, conformance );
    }


    @Override
    protected RelDataType getLogicalSourceRowType( RelDataType sourceRowType, SqlInsert insert ) {
        final RelDataType superType = super.getLogicalSourceRowType( sourceRowType, insert );
        return ((JavaTypeFactory) typeFactory).toSql( superType );
    }


    @Override
    protected RelDataType getLogicalTargetRowType( RelDataType targetRowType, SqlInsert insert ) {
        final RelDataType superType = super.getLogicalTargetRowType( targetRowType, insert );
        return ((JavaTypeFactory) typeFactory).toSql( superType );
    }

}
