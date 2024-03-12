/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.nodes.validate;

import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;

public interface ValidatorNamespace {

    /**
     * Returns the row type of this namespace, which comprises a list of names and types of the output columns. If the
     * scope's type has not yet been derived, derives it.
     *
     * @return Row type of this namespace, never null, always a struct
     */
    AlgDataType getTupleType();


    default DataModel getDataModel() {
        return DataModel.RELATIONAL;
    }

    default ValidatorNamespace setDataModel( DataModel dataModel ) {
        throw new UnsupportedOperationException( "This namespace does not support setting the data model." );
    }


}
