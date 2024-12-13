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

package org.polypheny.db.adapter;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.type.entity.PolyValue;
import java.util.ArrayList;

public interface NeoProcedureProvider {
    public Convention getConvention( AlgPlanner planner );
    public AlgNode getCall( AlgCluster cluster, AlgTraitSet traits, ArrayList<String> namespace, String procedureName, ArrayList<PolyValue> arguments, boolean yieldAll, ArrayList<String> yieldItems);
    public Adapter getStore();
}
