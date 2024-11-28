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

package org.polypheny.db.workflow.engine.storage;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

public class QueryUtils {

    private QueryUtils() {

    }


    public static Pair<ParsedQueryContext, AlgRoot> parseAndTranslateQuery( QueryContext context, Statement statement ) {
        ParsedQueryContext parsed = context.getLanguage().parser().apply( context ).get( 0 );
        Processor processor = context.getLanguage().processorSupplier().get();

        if ( parsed.getQueryNode().isEmpty() ) {
            throw new GenericRuntimeException( "Error during parsing of query \"%s\"".formatted( context.getQuery() ) );
        }

        if ( parsed.getLanguage().validatorSupplier() != null ) {
            Pair<Node, AlgDataType> validated = processor.validate(
                    context.getTransactions().get( 0 ),
                    parsed.getQueryNode().get(),
                    RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            parsed = ParsedQueryContext.fromQuery( parsed.getQuery(), validated.left, parsed );
        }
        AlgRoot root = processor.translate( statement, parsed );
        return Pair.of( parsed, root );
    }


    public static ExecutedContext executeQuery( Pair<ParsedQueryContext, AlgRoot> parsed, Statement statement ) {
        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( parsed.right, false );
        return new ImplementationContext( implementation, parsed.left, statement, null ).execute( statement );
    }


    public static boolean validateAlg( AlgRoot root, boolean allowDml, List<LogicalEntity> allowedEntities ) {
        Set<Long> allowedIds = allowedEntities == null ? null : allowedEntities.stream().map( e -> e.id ).collect( Collectors.toSet() );
        return validateRecursive( root.alg, allowDml, allowedIds );
    }


    private static boolean validateRecursive( AlgNode root, boolean allowDml, Set<Long> allowedIds ) {
        boolean checkEntities = allowedIds != null;
        if ( !allowDml ) {
            if ( root instanceof Modify ) {
                return false;
            }
        } else if ( checkEntities && root instanceof Modify<?> modify && !allowedIds.contains( modify.entity.id ) ) {
            // check if modify only has allowed entities
            return false;
        }

        // Check Scan
        if ( checkEntities && root instanceof Scan<?> scan && !allowedIds.contains( scan.entity.id ) ) {
            return false;
        }

        return root.getInputs().stream().allMatch( node -> validateRecursive( node, allowDml, allowedIds ) );
    }

}
