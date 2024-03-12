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

package org.polypheny.db.algebra.enumerable;


import java.lang.reflect.Type;
import java.util.List;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Information on the aggregate calculation context. {@link AggAddContext} provides basic static information on types of arguments and the return value of the aggregate being implemented.
 */
public interface AggContext {

    /**
     * Returns the aggregation being implemented.
     *
     * @return aggregation being implemented.
     */
    AggFunction aggregation();

    /**
     * Returns the return type of the aggregate as {@link AlgDataType}.
     * This can be helpful to test {@link AlgDataType#isNullable()}.
     *
     * @return return type of the aggregate
     */
    AlgDataType returnAlgType();

    /**
     * Returns the return type of the aggregate as {@link Type}.
     *
     * @return return type of the aggregate as {@link Type}
     */
    Type returnType();

    /**
     * Returns the parameter types of the aggregate as {@link AlgDataType}.
     * This can be helpful to test {@link AlgDataType#isNullable()}.
     *
     * @return Parameter types of the aggregate
     */
    List<? extends AlgDataType> parameterAlgTypes();

    /**
     * Returns the parameter types of the aggregate as {@link Type}.
     *
     * @return Parameter types of the aggregate
     */
    List<? extends Type> parameterTypes();

    /**
     * Returns the ordinals of the input fields that make up the key.
     */
    List<Integer> keyOrdinals();

    /**
     * Returns the types of the group key as {@link AlgDataType}.
     */
    List<? extends AlgDataType> keyAlgTypes();

    /**
     * Returns the types of the group key as {@link Type}.
     */
    List<? extends Type> keyTypes();

    /**
     * Returns the grouping sets we are aggregating on.
     */
    List<ImmutableBitSet> groupSets();

}

