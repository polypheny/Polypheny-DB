/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.transaction;

import java.util.List;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.QueryAnalyzer.StatementAnalyzer;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.FileInputHandle;

public interface Statement {

    Transaction getTransaction();

    QueryProcessor getQueryProcessor();

    DataContext getDataContext();

    Context getPrepareContext();

    InformationDuration getProcessingDuration();

    InformationDuration getRoutingDuration();

    InformationDuration getOverviewDuration();

    StatementEvent getMonitoringEvent();

    StatementAnalyzer getAnalyzer();

    boolean isAnalyze();


    default Integer getMaterializedDmlRowCount() {
        return null;
    }


    default void setMaterializedDmlRowCount( Integer count ) {
    }


    default boolean isInsertRowCaptureRequested() {
        return getInsertReturning() != null
                || (getGeneratedKeysProjection() != null && !getGeneratedKeysProjection().ordinals().isEmpty());
    }


    default InsertReturning getInsertReturning() {
        return null;
    }


    default void setInsertReturning( InsertReturning returning ) {
        throw new UnsupportedOperationException();
    }


    default void setInsertedRows( List<List<PolyValue>> rows ) {
        throw new UnsupportedOperationException();
    }


    default List<List<PolyValue>> getInsertedRows() {
        return List.of();
    }


    default GeneratedKeySelection getGeneratedKeySelection() {
        return null;
    }


    default void setGeneratedKeySelection( GeneratedKeySelection selection ) {
        throw new UnsupportedOperationException();
    }


    default InsertReturning getGeneratedKeysProjection() {
        return null;
    }


    default void setGeneratedKeysProjection( InsertReturning projection ) {
        throw new UnsupportedOperationException();
    }


    /** Empty names and indexes select every identity column. Column indexes are one-based. */
    record GeneratedKeySelection(List<String> names, List<Integer> indexes) {

        public GeneratedKeySelection {
            names = List.copyOf( names );
            indexes = List.copyOf( indexes );
        }

    }


    record InsertReturning(AlgDataType rowType, List<Integer> ordinals, List<List<String>> origins) {

    }

    /**
     * Get the index of this statement in the list of statements for that transaction
     *
     * @return the index this statement has in the statements list of the transaction
     */
    long getIndex();

    void setMonitoringEvent( StatementEvent event );

    void close();

    void registerFileInputHandle( FileInputHandle fileInputHandle );

}
