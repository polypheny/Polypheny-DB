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

package org.polypheny.db.prisminterface;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DocumentIndexProvider;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.prism.Document;
import org.polypheny.prism.DocumentField;
import org.polypheny.prism.DocumentIndex;

/**
 * Document collections have no declared field schema. Describe a bounded observation
 * of the collection in the caller's transaction, without committing its work.
 */
final class DocumentMetaRetriever {

    static final int SAMPLE_SIZE = 100;
    static final int MAX_FIELD_DEPTH = 32;


    private DocumentMetaRetriever() {
    }


    static Document getCollectionMeta( LogicalCollection collection, PIClient client ) {
        boolean ownsTransaction = client.hasNoTransaction();
        Transaction transaction = ownsTransaction ? client.createMetadataTransaction( collection.namespaceId ) : client.getCurrentTransaction();
        try {
            Document metadata = readCollectionMeta( collection, transaction );
            if ( ownsTransaction ) {
                transaction.commit();
            }
            return metadata;
        } catch ( TransactionException e ) {
            throw new PIServiceException( "Completing document metadata transaction failed: " + e.getMessage() );
        } finally {
            if ( ownsTransaction && transaction.isActive() ) {
                try {
                    transaction.rollback( "Document metadata request failed" );
                } catch ( TransactionException e ) {
                    throw new PIServiceException( "Rollback of document metadata transaction failed: " + e.getMessage() );
                }
            }
        }
    }


    private static Document readCollectionMeta( LogicalCollection collection, Transaction transaction ) {
        Statement statement = transaction.createStatement();
        try {
            Snapshot snapshot = statement.getDataContext().getSnapshot();
            RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
            AlgCluster cluster = AlgCluster.createDocument( statement.getQueryProcessor().getPlanner(), rexBuilder, snapshot );
            AlgNode scan = LogicalDocumentScan.create( cluster, collection );
            AlgNode limited = LogicalDocumentSort.create( scan, AlgCollations.EMPTY, List.of(), null,
                    rexBuilder.makeExactLiteral( BigDecimal.valueOf( SAMPLE_SIZE ) ) );
            PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( AlgRoot.of( limited, Kind.SELECT ), false );
            List<PolyDocument> documents = new ArrayList<>();
            try ( ResultIterator iterator = implementation.execute( statement, SAMPLE_SIZE ) ) {
                for ( List<PolyValue> row : iterator.getNextBatch( SAMPLE_SIZE ) ) {
                    documents.add( row.get( 0 ).asDocument() );
                }
            }
            return describeDocuments( snapshot.getNamespace( collection.namespaceId ).orElseThrow().name, collection.name, documents )
                    .addAllDocumentIndexes( getIndexes( collection, snapshot ) )
                    .build();
        } finally {
            statement.close();
        }
    }


    static Document.Builder describeDocuments( String namespaceName, String collectionName, List<PolyDocument> documents ) {
        Document.Builder result = Document.newBuilder().setNamespaceName( namespaceName ).setCollectionName( collectionName );
        Map<String, FieldObservation> fields = new TreeMap<>();
        for ( PolyDocument document : documents ) {
            observeFields( document, "", 0, fields, new HashSet<>() );
        }
        fields.forEach( (path, observation) -> result.addFields( DocumentField.newBuilder()
                .setFieldPath( path )
                .setTypeName( observation.type == null ? "null" : observation.type )
                .setIsOptional( observation.nullable || observation.present < documents.size() ) ) );
        if ( !documents.isEmpty() ) {
            result.setSampleDocumentJson( documents.get( 0 ).toJson() );
        }
        return result;
    }


    private static void observeFields( PolyDocument document, String prefix, int depth, Map<String, FieldObservation> fields, Set<String> seen ) {
        for ( Map.Entry<PolyString, PolyValue> entry : document.entrySet() ) {
            String path = prefix + escapePathSegment( entry.getKey().value );
            PolyValue value = entry.getValue();
            boolean missing = value != null && value.isDocument() && value.asDocument().isUnset;
            if ( missing ) {
                continue;
            }
            FieldObservation observation = fields.computeIfAbsent( path, p -> new FieldObservation() );
            if ( seen.add( path ) ) {
                observation.present++;
            }
            if ( value == null || value.isNull() ) {
                observation.nullable = true;
            } else {
                observation.type = mergeTypes( observation.type, typeName( value ) );
                if ( value.isDocument() && depth + 1 < MAX_FIELD_DEPTH ) {
                    observeFields( value.asDocument(), path + ".", depth + 1, fields, seen );
                }
            }
        }
    }


    private static String escapePathSegment( String segment ) {
        return segment.replace( "\\", "\\\\" ).replace( ".", "\\." );
    }


    private static String typeName( PolyValue value ) {
        return switch ( value.type ) {
            case CHAR, VARCHAR, TEXT -> "string";
            case TINYINT, SMALLINT, INTEGER, BIGINT -> "int";
            case FLOAT, REAL, DOUBLE, DECIMAL -> "double";
            case BOOLEAN -> "bool";
            case ARRAY -> "array";
            case DOCUMENT, MAP -> "object";
            default -> value.type.name().toLowerCase( Locale.ROOT );
        };
    }


    private static String mergeTypes( String previous, String next ) {
        if ( previous == null || previous.equals( next ) ) {
            return next;
        }
        if ( (previous.equals( "int" ) && next.equals( "double" )) || (previous.equals( "double" ) && next.equals( "int" )) ) {
            return "double";
        }
        return "mixed";
    }


    private static List<DocumentIndex> getIndexes( LogicalCollection collection, Snapshot snapshot ) {
        List<DocumentIndex> indexes = new ArrayList<>();
        snapshot.alloc().getFromLogical( collection.id ).forEach( allocation -> {
            var adapter = AdapterManager.getInstance().getAdapter( allocation.adapterId ).orElseThrow();
            List<PhysicalEntity> physicals = adapter.getAdapterCatalog().getPhysicalsFromAllocs( allocation.id );
            if ( physicals == null ) {
                return;
            }
            for ( PhysicalEntity physical : physicals ) {
                if ( physical instanceof DocumentIndexProvider provider ) {
                    provider.getDocumentIndexes().forEach( index -> indexes.add( DocumentIndex.newBuilder()
                            .setFieldPath( index.fieldPath() )
                            .setIndexType( index.indexType() )
                            .setUnique( index.unique() )
                            .build() ) );
                }
            }
        } );
        return indexes.stream().distinct().sorted( java.util.Comparator.comparing( DocumentIndex::getFieldPath )
                .thenComparing( DocumentIndex::getIndexType ).thenComparing( DocumentIndex::getUnique ) ).toList();
    }


    private static class FieldObservation {

        int present;
        boolean nullable;
        String type;

    }

}
