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

package org.polypheny.db.adaptiveness.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.AffectedOperations;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.ClauseCategory;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.ClauseName;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.Target;

public class ClausesRegister {


    @Getter
    private static boolean isInit = false;

    @Getter
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
                ClauseCategory.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If fully persistent is switched on, Polypheny only adds tables and partitions to persistent stores.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> l.stream().filter( e -> ((DataStore) e).isPersistent() ).collect( Collectors.toList() )) );
                }},
                new HashMap<>())
        );

        register( new BooleanClause(
                ClauseName.PERSISTENT,
                false,
                false,
                ClauseCategory.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If persistent is switched on, a table must be stored on at least one persistent store.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> {
                        if( l.stream().anyMatch( e -> ((DataStore) e).isPersistent() ) ){
                             return new ArrayList<>( l );
                        }
                        return Collections.emptyList();
                    }) );
                }},
                new HashMap<>())
        );

        register( new BooleanClause(
                ClauseName.ONLY_EMBEDDED,
                false,
                true,
                ClauseCategory.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If only embedded is switched on, Polypheny only adds tables and partitions to embedded store.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> l.stream().filter( e -> ((DataStore) e).getDeployMode() == DeployMode.EMBEDDED ).collect( Collectors.toList() )) );
                }},
                new HashMap<>())
        );

        register( new BooleanClause(
                ClauseName.ONLY_DOCKER,
                false,
                false,
                ClauseCategory.STORE,
                Arrays.asList( Target.POLYPHENY, Target.NAMESPACE, Target.ENTITY ),
                "If only docker is switched on, Polypheny only adds tables and partitions to docker store.",
                new HashMap<>() {{
                    put( AffectedOperations.STORE, (( l ) -> l.stream().filter( e -> ((DataStore) e).getDeployMode() == DeployMode.DOCKER ).collect( Collectors.toList() )) );
                }},
                new HashMap<>())
        );


        register( new BooleanClause(
                ClauseName.SPEED_OPTIMIZATION,
                false,
                false,
                ClauseCategory.SELF_ADAPTING,
                List.of( Target.POLYPHENY ),
                "Self adaptive options, to choose how the system should adapt itself, in this case speed optimization.",
                new HashMap<>(),
                new HashMap<>())
        );

        register( new BooleanClause(
                ClauseName.SPACE_OPTIMIZATION,
                false,
                false,
                ClauseCategory.SELF_ADAPTING,
                List.of( Target.POLYPHENY ),
                "Self adaptive options, to choose how the system should adapt itself, in this case space optimization.",
                new HashMap<>(),
                new HashMap<>())
        );
        register( new BooleanClause(
                ClauseName.REDUNDANCY_OPTIMIZATION,
                false,
                false,
                ClauseCategory.SELF_ADAPTING,
                List.of( Target.POLYPHENY ),
                "Self adaptive options, to choose how the system should adapt itself, in this case redundancy optimization.",
                new HashMap<>(),
                new HashMap<>())
        );
        register( new BooleanClause(
                ClauseName.LANGUAGE_OPTIMIZATION,
                false,
                false,
                ClauseCategory.SELF_ADAPTING,
                List.of( Target.POLYPHENY ),
                "Self adaptive options, to choose how the system should adapt itself, in this case query language optimization.",
                new HashMap<>(),
                new HashMap<>())
        );

        addInterferingClauses();
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

        BooleanClause fullyPersistent = registry.get( ClauseName.FULLY_PERSISTENT ).copyClause();
        BooleanClause minimalPersistent = registry.get( ClauseName.PERSISTENT ).copyClause();
        BooleanClause embedded = registry.get( ClauseName.ONLY_EMBEDDED ).copyClause();
        BooleanClause docker = registry.get( ClauseName.ONLY_DOCKER ).copyClause();

        //fully and minimal-persistent interfering
        fullyPersistent.setValue( true );
        minimalPersistent.setValue( true );
        Clause fullyPersistentClause = registry.get( ClauseName.FULLY_PERSISTENT );
        fullyPersistentClause.getInterfering().put(fullyPersistent , minimalPersistent);
        registry.put(fullyPersistentClause.getClauseName(), fullyPersistentClause );

        Clause persistentClause = registry.get( ClauseName.PERSISTENT );
        persistentClause.getInterfering().put(minimalPersistent, fullyPersistent);
        registry.put(persistentClause.getClauseName(), persistentClause );

        //embedded and docker persistent interfering
        embedded.setValue( true );
        docker.setValue( true );
        Clause embeddedClause = registry.get( ClauseName.ONLY_EMBEDDED );
        embeddedClause.getInterfering().put(embedded, docker);
        registry.put(embeddedClause.getClauseName(), embeddedClause );

        Clause dockerClause = registry.get( ClauseName.ONLY_DOCKER );
        dockerClause.getInterfering().put(docker, embedded);
        registry.put(dockerClause.getClauseName(), dockerClause );
    }

}
