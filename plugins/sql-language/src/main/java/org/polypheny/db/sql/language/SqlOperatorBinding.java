/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;


/**
 * <code>SqlOperatorBinding</code> represents the binding of an {@link SqlOperator} to actual operands, along with any additional information required to validate those operands if needed.
 */
public abstract class SqlOperatorBinding extends OperatorBinding {


    /**
     * Creates a SqlOperatorBinding.
     *
     * @param typeFactory Type factory
     * @param sqlOperator Operator which is subject of this call
     */
    protected SqlOperatorBinding( AlgDataTypeFactory typeFactory, Operator sqlOperator ) {
        super( typeFactory, sqlOperator );
    }


}

