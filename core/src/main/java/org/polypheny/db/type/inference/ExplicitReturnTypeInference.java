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

package org.polypheny.db.type.inference;


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.nodes.OperatorBinding;


/**
 * A {@link PolyReturnTypeInference} which always returns the same SQL type.
 */
public class ExplicitReturnTypeInference implements PolyReturnTypeInference {

    protected final AlgProtoDataType protoType;


    /**
     * Creates an inference rule which always returns the same type object.
     * <p>
     * If the requesting type factory is different, returns a copy of the type object made using
     * {@link AlgDataTypeFactory#copyType(AlgDataType)} within the requesting type factory.
     * <p>
     * A copy of the type is required because each statement is prepared using a different type factory; each type factory
     * maintains its own cache of canonical instances of each type.
     *
     * @param protoType Type object
     */
    protected ExplicitReturnTypeInference( AlgProtoDataType protoType ) {
        assert protoType != null;
        this.protoType = protoType;
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        return protoType.apply( opBinding.getTypeFactory() );
    }

}

