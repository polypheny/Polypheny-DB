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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.policies.policy.Clause.AffectedOperations;
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
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If fully persistent is switched on, Polypheny only adds tables and partitions to persistent stores.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> l.stream().filter( e -> ((DataStore) e).isPersistent() ).collect( Collectors.toList() )) );
                }})
        );

        register( new BooleanClause(
                ClauseName.PERSISTENT,
                false,
                false,
                Category.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If persistent is switched on, a table must be stored on at least one persistent store.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> {
                        if( l.stream().anyMatch( e -> ((DataStore) e).isPersistent() ) ){
                             return new ArrayList<>( l );
                        }
                        return Collections.emptyList();
                    }) );
                }} )
        );

        register( new BooleanClause(
                ClauseName.ONLY_EMBEDDED,
                false,
                true,
                Category.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If only embedded is switched on, Polypheny only adds tables and partitions to embedded store.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> l.stream().filter( e -> ((DataStore) e).getDeployMode() == DeployMode.EMBEDDED ).collect( Collectors.toList() )) );
                }})
        );

        register( new BooleanClause(
                ClauseName.ONLY_DOCKER,
                false,
                false,
                Category.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If only docker is switched on, Polypheny only adds tables and partitions to docker store.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> l.stream().filter( e -> ((DataStore) e).getDeployMode() == DeployMode.DOCKER ).collect( Collectors.toList() )) );
                }})
        );

    }


    private static void register( Clause clause ) {
        registry.put( clause.getClauseName(), clause );
    }


    public static Map<ClauseName, Clause> getBlankRegistry() {
        Map<ClauseName, Clause> registryCopy = new HashMap<>();
        for ( Clause value : registry.values() ) {
            registryCopy.put( value.getClauseName(), value.copyClause() );
        }
        return registryCopy;
    }


    public static void addInterferingClauses() {
        HashMap<Clause, Clause> interfering;


        BooleanClause fullyPersistent = (BooleanClause) registry.get( ClauseName.FULLY_PERSISTENT );
        BooleanClause minimalPersistent = (BooleanClause) registry.get( ClauseName.FULLY_PERSISTENT );
        BooleanClause embedded = (BooleanClause) registry.get( ClauseName.FULLY_PERSISTENT );
        BooleanClause docker = (BooleanClause) registry.get( ClauseName.FULLY_PERSISTENT );

        //fully persistent interfering
        fullyPersistent.setValue( true );
        minimalPersistent.setValue( true );
        fullyPersistent.getInterfering().put(fullyPersistent , minimalPersistent);

    }

}
