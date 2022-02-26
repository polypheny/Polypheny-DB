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

package org.polypheny.db.policies.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.policies.policy.Clause.Category;
import org.polypheny.db.policies.policy.Clause.ClauseName;
import org.polypheny.db.policies.policy.Policy.Target;

public class ClausesRegister {


    @Getter
    private static boolean isInit = false;

    private static final Map<ClauseName, Clause> registry = new HashMap<>();


    public static void registerClauses() {
        if ( isInit ) {
            throw new RuntimeException( "Clauses were already registered." );
        }
        isInit = true;

        register( new BooleanClause(
                        ClauseName.FULLY_PERSISTENT,
                        false, 
                        true,
                        Category.STORE,
                        Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY),
                        "If fully persistent is switched on, Polypheny only adds tables and partitions to persistent stores."  )
        );

        register( new BooleanClause(
                        ClauseName.PERSISTENT,
                        false,
                        false,
                        Category.STORE,
                        Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY),
                        "If persistent is switched on, a table must be stored on at least one persistent store."  )
        );

        register( new BooleanClause(
                        ClauseName.ONLY_EMBEDDED,
                        false,
                        true,
                        Category.STORE,
                        Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY),
                        "If only embedded is switched on, Polypheny only adds tables and partitions to embedded store."  )
        );

        register( new BooleanClause(
                        ClauseName.ONLY_DOCKER,
                        false,
                        false,
                        Category.STORE,
                        Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY),
                        "If only docker is switched on, Polypheny only adds tables and partitions to docker store."  )
        );

    }


    private static void register( Clause clause ) {
        registry.put( clause.getClauseName(), clause );
    }

    public static Map<ClauseName, Clause> getRegistry(){
        Map<ClauseName, Clause> registryCopy  = new HashMap<>();
        for ( Clause value : registry.values() ) {
            if(value instanceof BooleanClause){
                BooleanClause booleanClause = new BooleanClause(
                        value.getClauseName(),
                        ((BooleanClause)value).isValue(),
                        value.isDefault(),
                        value.getCategory(),
                        value.getPossibleTargets(),
                        value.getDescription()
                );
                registryCopy.put(booleanClause.getClauseName(), booleanClause );
            }
        }
        return registryCopy;
    }

}
