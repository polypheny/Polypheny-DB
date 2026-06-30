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

package org.polypheny.db.webui;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;


public final class SourceQueryReferenceDetector {

    private SourceQueryReferenceDetector() {
    }


    public static List<LogicalTable> referencedSourceTables( String query, String language, long namespaceId, Snapshot snapshot ) {
        Set<EntityReference> parsedReferences = parsedReferences( query, language, namespaceId );

        return snapshot.rel().getTables( (org.polypheny.db.catalog.logistic.Pattern) null, null ).stream()
                .filter( table -> table.entityType == EntityType.SOURCE )
                .filter( table -> parsedReferences.stream().anyMatch( reference -> referencesTable( reference, table, namespaceId, snapshot ) ) )
                .toList();
    }


    public static List<LogicalCollection> referencedSourceCollections( String query, String language, long namespaceId, Snapshot snapshot ) {
        Set<EntityReference> parsedReferences = parsedReferences( query, language, namespaceId );

        return snapshot.doc().getCollections( (org.polypheny.db.catalog.logistic.Pattern) null, null ).stream()
                .filter( collection -> collection.entityType == EntityType.SOURCE )
                .filter( collection -> parsedReferences.stream().anyMatch( reference -> referencesCollection( reference, collection, namespaceId, snapshot ) ) )
                .toList();
    }


    public static boolean referencesTable( String query, String language, LogicalTable table, long namespaceId, Snapshot snapshot ) {
        return parsedReferences( query, language, namespaceId ).stream()
                .anyMatch( reference -> referencesTable( reference, table, namespaceId, snapshot ) );
    }


    public static boolean referencesCollection( String query, String language, LogicalCollection collection, long namespaceId, Snapshot snapshot ) {
        return parsedReferences( query, language, namespaceId ).stream()
                .anyMatch( reference -> referencesCollection( reference, collection, namespaceId, snapshot ) );
    }


    private static Set<EntityReference> parsedReferences( String query, String language, long namespaceId ) {
        if ( query == null || query.isBlank() || language == null || language.isBlank() ) {
            return Set.of();
        }

        try {
            QueryContext context = QueryContext.builder()
                    .query( query )
                    .language( QueryLanguage.from( language ) )
                    .origin( "Source query reference detection" )
                    .namespaceId( namespaceId )
                    .build();

            Set<EntityReference> references = new HashSet<>();
            for ( ParsedQueryContext parsed : LanguageManager.toQueryNodes( context ) ) {
                parsed.getQueryNode().ifPresent( node -> collectReferences( node, references ) );
            }
            return references;
        } catch ( Throwable e ) {
            return Set.of();
        }
    }


    private static void collectReferences( Node node, Set<EntityReference> references ) {
        if ( node == null ) {
            return;
        }

        if ( isMqlNode( node ) ) {
            addNodeEntity( node, references );
            return;
        }

        if ( !(node instanceof Call call) ) {
            addNodeEntity( node, references );
            return;
        }

        List<Node> operands = call.getOperandList();
        switch ( node.getKind() ) {
            case SELECT -> {
                collectFrom( operand( operands, 2 ), references );
                collectNestedStatements( operands, references, 2 );
            }
            case INSERT -> {
                collectFrom( operand( operands, 1 ), references );
                collectReferences( operand( operands, 2 ), references );
            }
            case UPDATE, DELETE, MERGE -> {
                collectFrom( operand( operands, 0 ), references );
                collectNestedStatements( operands, references, 0 );
            }
            default -> collectNestedStatements( operands, references, -1 );
        }
    }


    private static void collectFrom( Node node, Set<EntityReference> references ) {
        if ( node == null ) {
            return;
        }

        if ( node instanceof Identifier identifier && !identifier.isStar() ) {
            references.add( EntityReference.fromIdentifier( identifier.getNames() ) );
            return;
        }

        if ( !(node instanceof Call call) ) {
            addNodeEntity( node, references );
            return;
        }

        List<Node> operands = call.getOperandList();
        switch ( node.getKind() ) {
            case AS -> collectFrom( operand( operands, 0 ), references );
            case JOIN -> {
                collectFrom( operand( operands, 0 ), references );
                collectFrom( operand( operands, 3 ), references );
                collectNestedStatements( operands, references, 0, 3 );
            }
            case SELECT -> collectReferences( node, references );
            default -> {
                addNodeEntity( node, references );
                collectNestedStatements( operands, references, -1 );
            }
        }
    }


    private static void collectNestedStatements( List<? extends Node> operands, Set<EntityReference> references, int... ignoredIndexes ) {
        for ( int i = 0; i < operands.size(); i++ ) {
            Node operand = operands.get( i );
            if ( operand == null || isIgnored( i, ignoredIndexes ) ) {
                continue;
            }
            if ( isStatementNode( operand ) || operand.getKind() == Kind.JOIN ) {
                collectReferences( operand, references );
            } else if ( operand instanceof Call call ) {
                collectNestedStatements( call.getOperandList(), references, -1 );
            } else {
                collectNestedStatements( operand.getInputs(), references, -1 );
            }
        }
    }


    private static void addNodeEntity( Node node, Set<EntityReference> references ) {
        String entity;
        try {
            entity = node.getEntity();
        } catch ( RuntimeException e ) {
            return;
        }
        if ( entity != null ) {
            references.add( new EntityReference( node.getNamespaceName(), entity ) );
        }
    }


    private static boolean isStatementNode( Node node ) {
        return switch ( node.getKind() ) {
            case SELECT, INSERT, UPDATE, DELETE, MERGE -> true;
            default -> false;
        };
    }


    private static boolean isMqlNode( Node node ) {
        return "mongo".equals( node.getLanguage().serializedName() );
    }


    private static boolean isIgnored( int index, int... ignoredIndexes ) {
        for ( int ignoredIndex : ignoredIndexes ) {
            if ( index == ignoredIndex ) {
                return true;
            }
        }
        return false;
    }


    private static Node operand( List<? extends Node> operands, int index ) {
        return index >= 0 && index < operands.size() ? operands.get( index ) : null;
    }


    private static boolean referencesTable( EntityReference reference, LogicalTable table, long namespaceId, Snapshot snapshot ) {
        return entityNameMatches( reference.entityName(), table.name )
                && namespaceMatches( reference.namespaceName(), table.namespaceId, namespaceId, snapshot );
    }


    private static boolean referencesCollection( EntityReference reference, LogicalCollection collection, long namespaceId, Snapshot snapshot ) {
        return entityNameMatches( reference.entityName(), collection.name )
                && namespaceMatches( reference.namespaceName(), collection.namespaceId, namespaceId, snapshot );
    }


    private static boolean namespaceMatches( String referencedNamespace, long entityNamespaceId, long currentNamespaceId, Snapshot snapshot ) {
        if ( referencedNamespace == null || referencedNamespace.isBlank() ) {
            return entityNamespaceId == currentNamespaceId;
        }

        return snapshot.getNamespace( entityNamespaceId )
                .map( namespace -> namespace.name.equalsIgnoreCase( referencedNamespace ) )
                .orElse( false );
    }


    private static boolean entityNameMatches( String referencedName, String entityName ) {
        return referencedName != null && referencedName.equalsIgnoreCase( entityName );
    }

    private record EntityReference( String namespaceName, String entityName ) {

        private static EntityReference fromIdentifier( List<String> names ) {
            if ( names.size() > 1 ) {
                return new EntityReference( names.get( names.size() - 2 ), names.get( names.size() - 1 ) );
            }
            return new EntityReference( null, names.get( 0 ) );
        }

    }

}
