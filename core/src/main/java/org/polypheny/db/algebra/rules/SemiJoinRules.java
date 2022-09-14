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

package org.polypheny.db.algebra.rules;

import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.rules.SemiJoinRule.JoinToSemiJoinRule;
import org.polypheny.db.algebra.rules.SemiJoinRule.ProjectToSemiJoinRule;

public interface SemiJoinRules {

    SemiJoinRule PROJECT = new ProjectToSemiJoinRule( Project.class, Join.class, Aggregate.class, AlgFactories.LOGICAL_BUILDER, "SemiJoinRule:project" );

    SemiJoinRule JOIN = new JoinToSemiJoinRule( Join.class, Aggregate.class, AlgFactories.LOGICAL_BUILDER, "SemiJoinRule:join" );

}
