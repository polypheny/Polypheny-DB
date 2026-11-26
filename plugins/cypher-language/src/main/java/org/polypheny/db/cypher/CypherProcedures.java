/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.cypher;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import org.polypheny.db.cypher.clause.CypherCall;
import org.polypheny.db.languages.CypherTableProcedureProvider;
import org.polypheny.db.languages.CypherTableProcedureProvider.Table;


public final class CypherProcedures {

    private static final List<CypherTableProcedureProvider> PROVIDERS = ServiceLoader.load(
                    CypherTableProcedureProvider.class, CypherTableProcedureProvider.class.getClassLoader() )
            .stream().map( ServiceLoader.Provider::get ).toList();


    private CypherProcedures() {
    }


    public static Optional<Table> find( String graphNamespace, CypherCall call ) {
        List<String> name = new ArrayList<>( call.getNamespace() );
        name.add( call.getName() );
        for ( CypherTableProcedureProvider provider : PROVIDERS ) {
            Optional<Table> table = provider.find( graphNamespace, name );
            if ( table.isPresent() ) {
                return table;
            }
        }
        return Optional.empty();
    }

}
