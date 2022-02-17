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

package org.polypheny.db.algebra.core;

import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Slf4j
@Getter
public class ConstraintEnforcer extends BiAlg {

    private final List<Class<? extends Exception>> exceptionClasses;
    private final List<String> exceptionMessages;


    @Override
    protected AlgDataType deriveRowType() {
        return left.getRowType();
    }


    /**
     * Left is the initial dml query, which modifies the entity
     * right is the control query, which tests if still all conditions are correct
     */
    public ConstraintEnforcer(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            AlgNode modify,
            AlgNode control,
            List<Class<? extends Exception>> exceptionClasses,
            List<String> exceptionMessages ) {
        super( cluster, traitSet, modify, control );
        this.exceptionClasses = exceptionClasses;
        this.exceptionMessages = exceptionMessages;
    }


    @Override
    public String algCompareString() {
        return "Constraint:(" + this.left.algCompareString() + ")->[" + right.algCompareString() + "]";
    }

}
