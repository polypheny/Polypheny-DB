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

package org.polypheny.db.sql.language.validate;


import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;


/**
 * A scope which contains nothing besides a few parameters. Like {@link EmptyScope} (which is its base class), it has no parent scope.
 *
 * @see ParameterNamespace
 */
public class ParameterScope extends EmptyScope {

    /**
     * Map from the simple names of the parameters to types of the parameters ({@link AlgDataType}).
     */
    private final Map<String, AlgDataType> nameToTypeMap;


    public ParameterScope( SqlValidatorImpl validator, Map<String, AlgDataType> nameToTypeMap ) {
        super( validator );
        this.nameToTypeMap = nameToTypeMap;
    }


    @Override
    public SqlQualified fullyQualify( SqlIdentifier identifier ) {
        return SqlQualified.create( this, 1, null, identifier );
    }


    @Override
    public SqlValidatorScope getOperandScope( SqlCall call ) {
        return this;
    }

}

