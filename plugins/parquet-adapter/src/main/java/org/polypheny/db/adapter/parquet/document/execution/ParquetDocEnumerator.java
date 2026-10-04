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

package org.polypheny.db.adapter.parquet.document.execution;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetEnumerator;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetGroupFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

/**
 * Document enumerator.
 * Reads Parquet rows and create from each row a single PolyDocument.
 */
public class ParquetDocEnumerator extends AbstractParquetEnumerator implements Enumerator<PolyValue[]> {

    private static final ParquetDocValueExtractor documentValueExtractor = new ParquetDocValueExtractor();
    private final String documentPrefix;


    public ParquetDocEnumerator( ParquetSourceReader reader, FiltersContainer filtersContainer ) {
        this( reader, filtersContainer, true );
    }


    public ParquetDocEnumerator( ParquetSourceReader reader, FiltersContainer filtersContainer, boolean readerOwner ) {
        super( reader, filtersContainer, new ParquetGroupFilterEvaluator( reader.getProjectionSchema(), documentValueExtractor ), readerOwner );
        this.documentPrefix = reader.getSource().path();
    }


    /**
     * Creates document from parquet file row
     *
     * @param group - parquet file row
     * @return document converted to PolyValue list
     */
    @Override
    protected PolyValue[] extractRow( Group group ) {
        PolyDocument document = documentValueExtractor.extractDocument(
                group,
                reader.getProjectionSchema(),
                PolyString.of( documentPrefix + "#" + reader.getCurrentRowNumber() ) );
        return new PolyValue[]{ document };
    }

}
