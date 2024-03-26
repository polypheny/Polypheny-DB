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

package org.polypheny.db.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

@Value
@NonFinal
@SuperBuilder(toBuilder = true)
public class QueryContext {


    @NotNull
    String query;

    @NotNull
    QueryLanguage language;

    @Builder.Default
    boolean isAnalysed = false;

    @Builder.Default
    boolean usesCache = true;

    @Builder.Default
    long userId = Catalog.defaultUserId;

    @Builder.Default
    Statement statement = null;

    @NotNull
    String origin;

    @Builder.Default
    int batch = -1; // -1 for all

    TransactionManager transactionManager;

    @Builder.Default
    @NotNull
    Consumer<InformationManager> informationTarget = i -> {
    };

    @Builder.Default
    long namespaceId = Catalog.defaultNamespaceId;

    // we can have mixed transactions, which have ddls and dmls, as long as we commit instantly for ddls,
    // we have to open a new transaction for the next statement, so we need to keep track of all transactions (in theory only the last one is needed)
    @Builder.Default
    List<Transaction> transactions = new ArrayList<>();


    @EqualsAndHashCode(callSuper = true)
    @Value
    @SuperBuilder(toBuilder = true)
    public static class ParsedQueryContext extends QueryContext {

        @Nullable Node queryNode;


        public static ParsedQueryContext fromQuery( String query, Node queryNode, QueryContext context ) {
            long namespaceId = context.namespaceId;

            if ( queryNode != null && queryNode.getNamespaceName() != null ) {
                namespaceId = Catalog.snapshot().getNamespace( queryNode.getNamespaceName() ).map( n -> n.id ).orElse( queryNode.getNamespaceId() );
            }

            return ParsedQueryContext.builder()
                    .query( query )
                    .queryNode( queryNode )
                    .language( context.language )
                    .isAnalysed( context.isAnalysed )
                    .usesCache( context.usesCache )
                    .userId( context.userId )
                    .origin( context.getOrigin() )
                    .batch( context.batch )
                    .namespaceId( namespaceId )
                    .transactions( context.transactions )
                    .transactionManager( context.transactionManager )
                    .informationTarget( context.informationTarget ).build();
        }


        public Optional<Node> getQueryNode() {
            return Optional.ofNullable( queryNode );
        }

    }


    public <T extends QueryContext> T addTransaction( Transaction transaction ) {
        transactions.add( transaction );
        return (T) this;
    }

}
