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

package org.polypheny.db.transaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.processing.DataContextImpl;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.processing.QueryProviderImpl;
import org.polypheny.db.processing.VolcanoQueryProcessor;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.util.FileInputHandle;

public class StatementImpl implements Statement {

    private static final AtomicLong STATEMENT_COUNTER = new AtomicLong();

    @Getter
    private final long id;
    @Getter
    private final TransactionImpl transaction;
    private final List<FileInputHandle> fileInputHandles = new ArrayList<>();

    private QueryProcessor queryProcessor;
    private DataContext dataContext;
    private ContextImpl prepareContext;
    private InformationDuration processingDuration;
    private InformationDuration routingDuration;
    private InformationDuration overviewDuration;
    private InformationPage executionTimePage;

    private StatementEvent statementEvent;


    StatementImpl( TransactionImpl transaction ) {
        this.id = STATEMENT_COUNTER.getAndIncrement();
        this.transaction = transaction;
    }


    @Override
    public QueryProcessor getQueryProcessor() {
        if ( queryProcessor == null ) {
            queryProcessor = new VolcanoQueryProcessor( this );
        }
        return queryProcessor;
    }


    @Override
    public DataContext getDataContext() {
        if ( dataContext == null ) {
            Map<String, Object> map = new LinkedHashMap<>();
            // Avoid overflow
            int queryTimeout = RuntimeConfig.QUERY_TIMEOUT.getInteger();
            if ( queryTimeout > 0 && queryTimeout < Integer.MAX_VALUE / 1000 ) {
                map.put( DataContext.Variable.TIMEOUT.camelName, PolyLong.of( queryTimeout * 1000L ) );
            }

            final AtomicBoolean cancelFlag;
            cancelFlag = transaction.getCancelFlag();
            map.put( DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag );
            dataContext = new DataContextImpl(
                    new QueryProviderImpl(),
                    map,
                    transaction.getSnapshot(),
                    transaction.getTypeFactory(),
                    this );
        }
        return dataContext;
    }


    @Override
    public ContextImpl getPrepareContext() {
        if ( prepareContext == null ) {
            prepareContext = new ContextImpl(
                    transaction.getSnapshot(),
                    getDataContext(),
                    transaction.getDefaultNamespace().name,
                    transaction.getUser().id,
                    this );
        }
        return prepareContext;
    }


    @Override
    public InformationDuration getProcessingDuration() {
        if ( processingDuration == null ) {
            processingDuration = initDuration( "Query Processing", 2 );
        }
        return processingDuration;
    }


    @Override
    public InformationDuration getRoutingDuration() {
        if ( routingDuration == null ) {
            routingDuration = initDuration( "Query Routing", 3 );
        }
        return routingDuration;
    }


    @Override
    public InformationDuration getOverviewDuration() {
        if ( overviewDuration == null ) {
            overviewDuration = initDuration( "Overview", 1 );
        }
        return overviewDuration;
    }


    @Override
    public StatementEvent getMonitoringEvent() {
        return this.statementEvent;
    }


    @Override
    public void setMonitoringEvent( StatementEvent event ) {
        this.statementEvent = event;
    }


    private InformationDuration initDuration( String title, int order ) {
        InformationManager im = transaction.getQueryAnalyzer();
        if ( executionTimePage == null ) {
            executionTimePage = new InformationPage( "Execution Time", "Query processing & execution time" );
            im.addPage( executionTimePage );
        }
        InformationGroup group = new InformationGroup( executionTimePage, title );
        group.setOrder( order );
        im.addGroup( group );
        InformationDuration duration = new InformationDuration( group );
        im.registerInformation( duration );
        return duration;
    }


    @Override
    public void close() {
        prepareContext = null;
        if ( dataContext != null ) {
            dataContext.getParameterValues().clear();
        }
        fileInputHandles.forEach( FileInputHandle::close );
        // dataContext = null; todo dl check how we want to handle this longterm
    }


    @Override
    public void registerFileInputHandle( FileInputHandle fileInputHandle ) {
        fileInputHandles.add( fileInputHandle );
    }

}
