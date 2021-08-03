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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.jdbc.ContextImpl;
import org.polypheny.db.processing.DataContextImpl;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.processing.QueryProviderImpl;
import org.polypheny.db.processing.VolcanoQueryProcessor;
import org.polypheny.db.routing.RouterManager;
import org.polypheny.db.routing.Router;
import org.polypheny.db.util.FileInputHandle;

public class StatementImpl implements Statement {

    private static final AtomicLong STATEMENT_COUNTER = new AtomicLong();

    @Getter
    private final long id;

    @Getter
    private final TransactionImpl transaction;

    private QueryProcessor queryProcessor;

    private DataContext dataContext = null;
    private ContextImpl prepareContext = null;
    private final List<FileInputHandle> fileInputHandles = new ArrayList<>();

    private InformationDuration duration;


    StatementImpl( TransactionImpl transaction ) {
        this.id = STATEMENT_COUNTER.getAndIncrement();
        this.transaction = transaction;
        this.duration = null;
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
                map.put( DataContext.Variable.TIMEOUT.camelName, queryTimeout * 1000L );
            }

            final AtomicBoolean cancelFlag;
            cancelFlag = transaction.getCancelFlag();
            map.put( DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag );
            if ( RuntimeConfig.SPARK_ENGINE.getBoolean() ) {
                return new SlimDataContext();
            }
            dataContext = new DataContextImpl( new QueryProviderImpl(), map, transaction.getSchema(), transaction.getTypeFactory(), this );
        }
        return dataContext;
    }


    @Override
    public ContextImpl getPrepareContext() {
        if ( prepareContext == null ) {
            prepareContext = new ContextImpl(
                    transaction.getSchema(),
                    getDataContext(),
                    transaction.getDefaultSchema().name,
                    transaction.getDatabase().id,
                    transaction.getUser().id,
                    this );
        }
        return prepareContext;
    }


    @Override
    public InformationDuration getDuration() {
        if ( duration == null ) {
            InformationManager im = transaction.getQueryAnalyzer();
            InformationPage executionTimePage = new InformationPage( "Execution time", "Query execution time" );
            im.addPage( executionTimePage );
            InformationGroup group = new InformationGroup( executionTimePage, "Processing" );
            im.addGroup( group );
            duration = new InformationDuration( group );
            im.registerInformation( duration );
        }
        return duration;
    }


    @Override
    public List<Router> getRouters() {
        return RouterManager.getInstance().getRouters();
    }



    @Override
    public void close() {
        prepareContext = null;
        if ( dataContext != null ) {
            dataContext.getParameterValues().clear();
        }
        fileInputHandles.forEach( FileInputHandle::close );
        dataContext = null;
    }


    @Override
    public void registerFileInputHandle( FileInputHandle fileInputHandle ) {
        fileInputHandles.add( fileInputHandle );
    }

}
