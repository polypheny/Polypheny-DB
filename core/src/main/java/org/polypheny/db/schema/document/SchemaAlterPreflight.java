/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.schema.document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import org.bson.BsonDocument;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.schema.document.SchemaValidator.Violation;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Preflight validation for ALTER SCHEMA.
 */
public final class SchemaAlterPreflight {

    private static final int FETCH_SIZE = 10_000;
    private static final int MAX_SAMPLE_SIZE = 16;


    private SchemaAlterPreflight() {
    }


    public static SchemaAlterPreflightReport run( Catalog catalog, LogicalCollection collection, DocumentSchema targetSchema, Statement statement ) {

        List<?> allocations = new ArrayList<>( catalog.getSnapshot().alloc().getFromLogical( collection.id ) );
        if ( allocations.isEmpty() ) {
            return new SchemaAlterPreflightReport( true, 0, 0, List.of() );
        }

        LongAdder scanned = new LongAdder();
        LongAdder failing = new LongAdder();
        List<Violation> sample = new ArrayList<>( MAX_SAMPLE_SIZE );

        String mql = "db." + collection.name + ".find({})";

        QueryContext queryContext = QueryContext.builder().query( mql ).language( QueryLanguage.from( "mql" ) ).origin( "SchemaAlterPreflight" ).statement( statement ).namespaceId( collection.namespaceId ).build().addTransaction( statement.getTransaction() );

        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( queryContext );
        for ( ExecutedContext executedContext : executedContexts ) {
            if ( executedContext.getException().isPresent() ) {
                Throwable exception = executedContext.getException().get();
                throw new RuntimeException( "Document scan failed: " + exception.getMessage(), exception );
            }

            ResultIterator iterator = executedContext.getIterator();

            try {
                scanIterator( iterator, targetSchema, scanned, failing, sample );
            } finally {
                closeIteratorQuietly( iterator );
            }
        }

        return new SchemaAlterPreflightReport( failing.sum() == 0L, scanned.sum(), failing.sum(), sample );
    }


    private static void scanIterator( ResultIterator iterator, DocumentSchema targetSchema, LongAdder scanned, LongAdder failing, List<Violation> sample ) {

        while ( true ) {
            List<List<PolyValue>> batch = iterator.getNextBatch( FETCH_SIZE );
            if ( batch.isEmpty() ) {
                break;
            }

            for ( List<PolyValue> row : batch ) {
                processRow( row, targetSchema, scanned, failing, sample );
            }
        }
    }


    private static void processRow( List<PolyValue> row, DocumentSchema targetSchema, LongAdder scanned, LongAdder failing, List<Violation> sample ) {

        String json = extractJson( row );
        if ( json == null ) {
            return;
        }

        BsonDocument document = parseDocument( json, failing, sample );
        if ( document == null ) {
            return;
        }

        scanned.increment();

        BsonDocument documentForValidation = removeDocumentIdIfPresent( document );
        SchemaValidator.ValidationResult validationResult = SchemaValidator.validate( targetSchema, documentForValidation );

        if ( !validationResult.ok() ) {
            failing.increment();
            addSampleViolations( sample, validationResult.violations() );
        }
    }


    private static String extractJson( List<PolyValue> row ) {
        if ( row == null || row.isEmpty() ) {
            return null;
        }

        PolyValue value = row.get( 0 );

        try {
            return value.toJson();
        } catch ( Throwable throwable ) {
            String stringValue = String.valueOf( value ).trim();
            if ( !(stringValue.startsWith( "{" ) || stringValue.startsWith( "[" )) ) {
                return null;
            }
            return stringValue;
        }
    }


    private static BsonDocument parseDocument( String json, LongAdder failing, List<Violation> sample ) {
        try {
            return BsonDocument.parse( json );
        } catch ( Exception ignored ) {
            failing.increment();
            addSampleViolation( sample, new Violation( "$", "notValidJson", "Unparseable JSON row" ) );
            return null;
        }
    }


    private static BsonDocument removeDocumentIdIfPresent( BsonDocument document ) {
        if ( !document.containsKey( DocumentType.DOCUMENT_ID ) ) {
            return document;
        }

        BsonDocument clone = document.clone();
        clone.remove( DocumentType.DOCUMENT_ID );
        return clone;
    }


    private static void addSampleViolation( List<Violation> sample, Violation violation ) {
        if ( sample.size() < MAX_SAMPLE_SIZE ) {
            sample.add( violation );
        }
    }


    private static void addSampleViolations( List<Violation> sample, List<Violation> violations ) {
        if ( sample.size() >= MAX_SAMPLE_SIZE || violations.isEmpty() ) {
            return;
        }

        int remainingCapacity = MAX_SAMPLE_SIZE - sample.size();
        sample.addAll( violations.subList( 0, Math.min( violations.size(), remainingCapacity ) ) );
    }


    private static void closeIteratorQuietly( ResultIterator iterator ) {
        try {
            iterator.close();
        } catch ( Throwable ignored ) {
            // Ignore close failures.
        }
    }

}