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
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.util.AccessType;


/**
 * Implements {@link ValidatorTable} by delegating to a parent table.
 */
public abstract class DelegatingSqlValidatorTable implements ValidatorTable {

    protected final ValidatorTable table;


    /**
     * Creates a DelegatingSqlValidatorTable.
     *
     * @param table Parent table
     */
    public DelegatingSqlValidatorTable( ValidatorTable table ) {
        this.table = table;
    }


    @Override
    public AlgDataType getRowType() {
        return table.getRowType();
    }


    @Override
    public List<String> getQualifiedName() {
        return table.getQualifiedName();
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        return table.getMonotonicity( columnName );
    }


    @Override
    public AccessType getAllowedAccess() {
        return table.getAllowedAccess();
    }

}

