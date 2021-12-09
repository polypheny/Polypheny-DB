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

package org.polypheny.db;

import static org.reflections.Reflections.log;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.interpreter.BindableConvention;
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
public class PolyResult {

    public final AlgDataType rowType;
    private final long maxRowCount = -1;
    private final Kind kind;
    private Bindable<Object[]> bindable;
    private final SchemaType schemaType;
    private final ExecutionTimeMonitor executionTimeMonitor;
    private CursorFactory cursorFactory;
    private final Convention resultConvention;
    private List<ColumnMetaData> columns;
    private final PreparedResult preparedResult;
    private final Statement statement;
    @Getter
    @Accessors(fluent = true)
    private boolean hasMoreRows;


    /**
     * {@link PolyResult} should serve as a jack-of-all-trades results implementation of the results of a query.
     * It should minimize the needed variables to be instantiated and defer access of more complex information
     * on access e.g. {@link #getColumns()}
     *
     * @param rowType
     * @param schemaType
     * @param executionTimeMonitor
     * @param preparedResult
     * @param kind
     * @param statement
     * @param resultConvention
     */
    public PolyResult(
            AlgDataType rowType,
            SchemaType schemaType,
            ExecutionTimeMonitor executionTimeMonitor,
            PreparedResult preparedResult,
            Kind kind,
            Statement statement,
            Convention resultConvention ) {
        this.rowType = rowType;
        this.schemaType = schemaType;
        this.executionTimeMonitor = executionTimeMonitor;
        this.preparedResult = preparedResult;
        this.kind = kind;
        this.statement = statement;
        this.resultConvention = resultConvention;
        if ( Kind.DDL.contains( kind ) ) {
            this.columns = ImmutableList.of();
        }
    }


    public Enumerable<?> enumerable( DataContext dataContext ) {
        return getBindable().bind( dataContext );
    }


    public <T> Enumerable<T> enumerable( DataContext dataContext, Class<T> clazz ) {
        return (Enumerable<T>) getBindable().bind( dataContext );
    }


    public List<List<Object>> getRows( Statement statement, int size ) {
        return getRows( statement, size, false, false );
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


    public Bindable<?> getBindable() {
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


    public List<List<Object>> getRows( Statement statement, int size, boolean isTimed, boolean isAnalyzed ) {
        Iterator<Object> iterator = null;
        StopWatch stopWatch = null;
        try {
            if ( isAnalyzed ) {
                statement.getOverviewDuration().start( "Execution" );
            }
            final Enumerable<Object> enumerable = enumerable( statement.getDataContext(), Object.class );
            if ( isAnalyzed ) {
                statement.getOverviewDuration().stop( "Execution" );
            }

            iterator = enumerable.iterator();
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

}