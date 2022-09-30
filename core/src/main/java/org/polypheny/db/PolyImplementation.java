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

package org.polypheny.db;

import static org.reflections.Reflections.log;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.processing.QueryProcessorHelpers;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.LimitIterator;


@Getter
public class PolyImplementation {

    public final AlgDataType rowType;
    private final long maxRowCount = -1;
    private final Kind kind;
    private Bindable<Object> bindable;
    private final NamespaceType namespaceType;
    private final ExecutionTimeMonitor executionTimeMonitor;
    private CursorFactory cursorFactory;
    private final Convention resultConvention;
    private List<ColumnMetaData> columns;
    private final PreparedResult preparedResult;
    private final Statement statement;
    @Accessors(fluent = true)
    private boolean hasMoreRows;
    @Accessors(fluent = true)
    private final boolean isDDL;


    /**
     * {@link PolyImplementation} should serve as a jack-of-all-trades results implementation of the results of a query.
     * It should minimize the needed variables to be instantiated and defer access of more complex information
     * on access e.g. {@link #getColumns()}
     *
     * @param rowType defines the types of the result
     * @param namespaceType type of the
     * @param executionTimeMonitor to keep track of different execution times
     * @param preparedResult nullable result, which holds all info from the execution
     * @param kind of initial query, which is used to get type of result e.g. DDL, DQL,...
     * @param statement used statement for this result
     * @param resultConvention the nullable result convention
     */
    public PolyImplementation(
            @Nullable AlgDataType rowType,
            NamespaceType namespaceType,
            ExecutionTimeMonitor executionTimeMonitor,
            @Nullable PreparedResult preparedResult,
            Kind kind,
            Statement statement,
            @Nullable Convention resultConvention ) {
        this.rowType = rowType;
        this.namespaceType = namespaceType;
        this.executionTimeMonitor = executionTimeMonitor;
        this.preparedResult = preparedResult;
        this.kind = kind;
        this.statement = statement;
        this.resultConvention = resultConvention;
        this.isDDL = Kind.DDL.contains( kind );
        if ( this.isDDL ) {
            this.columns = ImmutableList.of();
        }
    }


    public Enumerable<Object> enumerable( DataContext dataContext ) {
        return enumerable( getBindable(), dataContext );
    }


    public static <T> Enumerable<T> enumerable( Bindable<T> bindable, DataContext dataContext ) {
        return bindable.bind( dataContext );
    }


    public Class<?> getResultClass() {
        Class<?> resultClazz = null;
        if ( preparedResult instanceof Typed ) {
            resultClazz = (Class<?>) ((Typed) preparedResult).getElementType();
        }
        return resultClazz;
    }


    public CursorFactory getCursorFactory() {
        if ( cursorFactory != null ) {
            return cursorFactory;
        }
        if ( resultConvention == null ) {
            return Meta.CursorFactory.OBJECT;
        }

        cursorFactory = resultConvention == BindableConvention.INSTANCE
                ? CursorFactory.ARRAY
                : CursorFactory.deduce( getColumns(), getResultClass() );

        return cursorFactory;
    }


    public Bindable<Object> getBindable() {
        if ( Kind.DDL.contains( kind ) ) {
            return null;
        }

        if ( bindable != null ) {
            return bindable;
        }
        bindable = preparedResult.getBindable( getCursorFactory() );
        return bindable;
    }


    public List<ColumnMetaData> getColumns() {
        if ( columns != null ) {
            return columns;
        }

        final AlgDataType x;
        switch ( kind ) {
            case INSERT:
            case DELETE:
            case UPDATE:
            case EXPLAIN:
                // FIXME: getValidatedNodeType is wrong for DML
                x = AlgOptUtil.createDmlRowType( kind, statement.getTransaction().getTypeFactory() );
                break;
            default:
                x = rowType;
        }
        final List<ColumnMetaData> columns = QueryProcessorHelpers.getColumnMetaDataList(
                statement.getTransaction().getTypeFactory(),
                x,
                QueryProcessorHelpers.makeStruct( statement.getTransaction().getTypeFactory(), x ),
                preparedResult.getFieldOrigins() );

        this.columns = columns;
        return columns;

    }


    public List<List<Object>> getRows( Statement statement, int size ) {
        return getRows( statement, size, false, false );
    }


    public List<List<Object>> getRows( Statement statement, int size, boolean isTimed, boolean isAnalyzed ) {
        return getRows( statement, size, isTimed, isAnalyzed, null, false );
    }


    public List<List<Object>> getRows( Statement statement, int size, boolean isTimed, boolean isAnalyzed, StatementEvent statementEvent, boolean isIndex ) {
        Iterator<Object> iterator = null;
        StopWatch stopWatch = null;
        try {
            iterator = createIterator( getBindable(), statement, isAnalyzed );
            List<List<Object>> res;

            if ( isTimed ) {
                stopWatch = new StopWatch();
                stopWatch.start();
            }
            if ( size != -1 ) {
                res = MetaImpl.collect( cursorFactory, LimitIterator.of( iterator, size ), new ArrayList<>() );
            } else {
                res = MetaImpl.collect( cursorFactory, iterator, new ArrayList<>() );
            }
            this.hasMoreRows = iterator.hasNext();
            if ( isTimed ) {
                stopWatch.stop();
                executionTimeMonitor.setExecutionTime( stopWatch.getNanoTime() );
            }

            // Only if it is an index
            if ( statementEvent != null && isIndex ) {
                statementEvent.setIndexSize( res.size() );
            }

            return res;
        } catch ( Throwable t ) {
            if ( iterator != null ) {
                try {
                    if ( iterator instanceof AutoCloseable ) {
                        ((AutoCloseable) iterator).close();
                    }
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
            throw new RuntimeException( t );
        } finally {
            if ( iterator != null ) {
                try {
                    if ( iterator instanceof AutoCloseable ) {
                        ((AutoCloseable) iterator).close();
                    }
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
        }
    }


    private static Iterator<Object> createIterator( Bindable<Object> bindable, Statement statement, boolean isAnalyzed ) {
        if ( isAnalyzed ) {
            statement.getOverviewDuration().start( "Execution" );
        }
        final Enumerable<Object> enumerable = enumerable( bindable, statement.getDataContext() );
        if ( isAnalyzed ) {
            statement.getOverviewDuration().stop( "Execution" );
        }

        return enumerable.iterator();
    }


    public static Meta.StatementType toStatementType( Kind kind ) {
        if ( kind == Kind.SELECT ) {
            return Meta.StatementType.SELECT;
        } else if ( Kind.DDL.contains( kind ) ) {
            return Meta.StatementType.OTHER_DDL;
        } else if ( Kind.DML.contains( kind ) ) {
            return Meta.StatementType.IS_DML;
        }

        throw new RuntimeException( "Statement type does not exist." );
    }


    public StatementType getStatementType() {
        return toStatementType( this.kind );
    }


    public int getRowsChanged( Statement statement ) throws Exception {
        if ( Kind.DDL.contains( getKind() ) ) {
            return 1;
        } else if ( Kind.DML.contains( getKind() ) ) {
            int rowsChanged;
            try {
                Iterator<?> iterator = enumerable( statement.getDataContext() ).iterator();
                rowsChanged = getRowsChanged( statement, iterator, getKind().name() );
            } catch ( RuntimeException e ) {
                if ( e.getCause() != null ) {
                    throw new Exception( e.getCause().getMessage(), e );
                } else {
                    throw new Exception( e.getMessage(), e );
                }
            }
            return rowsChanged;
        } else {
            throw new Exception( "Unknown result type: " + getKind() );
        }
    }


    public static int getRowsChanged( Statement statement, Iterator<?> iterator, String kind ) throws Exception {
        int rowsChanged = -1;
        Object object;
        while ( iterator.hasNext() ) {
            object = iterator.next();
            int num;
            if ( object != null && object.getClass().isArray() ) {
                Object[] o = (Object[]) object;
                num = ((Number) o[0]).intValue();
            } else if ( object != null ) {
                num = ((Number) object).intValue();
            } else {
                throw new Exception( "Result is null" );
            }
            // Check if num is equal for all adapters
            if ( rowsChanged != -1 && rowsChanged != num ) {
                //throw new QueryExecutionException( "The number of changed rows is not equal for all stores!" );
            }
            rowsChanged = num;
        }

        addMonitoringInformation( statement, kind, rowsChanged );

        // Some stores do not correctly report the number of changed rows (set to zero to avoid assertion error in the MetaResultSet.count() method)
        if ( rowsChanged < 0 ) {
            rowsChanged = 0;
        }

        return rowsChanged;
    }


    public static void addMonitoringInformation( Statement statement, String kind, int rowsChanged ) {
        StatementEvent eventData = statement.getMonitoringEvent();
        if ( rowsChanged > 0 ) {
            eventData.setRowCount( rowsChanged );
        }
        if ( Kind.INSERT.name().equals( kind ) || Kind.DELETE.name().equals( kind ) ) {

            HashMap<Long, List<Object>> ordered = new HashMap<>();

            List<Map<Long, Object>> values = statement.getDataContext().getParameterValues();
            if ( values.size() > 0 ) {
                for ( long i = 0; i < statement.getDataContext().getParameterValues().get( 0 ).size(); i++ ) {
                    ordered.put( i, new ArrayList<>() );
                }
            }

            for ( Map<Long, Object> longObjectMap : statement.getDataContext().getParameterValues() ) {
                longObjectMap.forEach( ( k, v ) -> {
                    ordered.get( k ).add( v );
                } );
            }

            eventData.getChangedValues().putAll( ordered );
            if ( Kind.INSERT.name().equals( kind ) ) {
                if ( rowsChanged >= 0 ) {
                    eventData.setRowCount( statement.getDataContext().getParameterValues().size() );
                }
            }
        }
    }

}
