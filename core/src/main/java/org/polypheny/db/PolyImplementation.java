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

package org.polypheny.db;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.processing.QueryProcessorHelpers;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyInteger;


@Getter
@Slf4j
public class PolyImplementation {

    public final AlgDataType tupleType;
    private final long maxRowCount = -1;
    private final Kind kind;
    private Bindable<PolyValue[]> bindable;
    private final DataModel dataModel;
    private final ExecutionTimeMonitor executionTimeMonitor;
    private CursorFactory cursorFactory;
    private final Convention resultConvention;
    private List<ColumnMetaData> fields;
    private final PreparedResult<PolyValue> preparedResult;
    private final Statement statement;

    @Accessors(fluent = true)
    private final boolean isDDL;
    private Iterator<PolyValue[]> iterator;


    /**
     * {@link PolyImplementation} should serve as a jack-of-all-trades results implementation of the results of a query.
     * It should minimize the needed variables to be instantiated and defer access of more complex information
     * on access e.g. {@link #getFields()}
     *
     * @param tupleType defines the types of the result
     * @param dataModel type of the
     * @param executionTimeMonitor to keep track of different execution times
     * @param preparedResult nullable result, which holds all info from the execution
     * @param kind of initial query, which is used to get type of result e.g. DDL, DQL,...
     * @param statement used statement for this result
     * @param resultConvention the nullable result convention
     */
    public PolyImplementation(
            @Nullable AlgDataType tupleType,
            DataModel dataModel,
            ExecutionTimeMonitor executionTimeMonitor,
            @Nullable PreparedResult<PolyValue> preparedResult,
            Kind kind,
            Statement statement,
            @Nullable Convention resultConvention ) {

        this.dataModel = dataModel;
        this.executionTimeMonitor = executionTimeMonitor;
        this.preparedResult = preparedResult;
        this.kind = kind;
        this.statement = statement;
        this.resultConvention = resultConvention;
        this.isDDL = Kind.DDL.contains( kind );

        if ( this.isDDL ) {
            this.fields = ImmutableList.of();
            Builder builder = statement.getTransaction().getTypeFactory().builder();
            builder.add( "ROWTYPE", null, PolyType.BIGINT );
            this.tupleType = builder.build();
        } else {
            this.tupleType = tupleType;
        }
    }


    public Enumerable<PolyValue[]> enumerable( DataContext dataContext ) {
        return enumerable( getBindable(), dataContext );
    }


    public static Enumerable<PolyValue[]> enumerable( Bindable<PolyValue[]> bindable, DataContext dataContext ) {
        return bindable.bind( dataContext );
    }


    public static <T> Enumerable<Object> enumerable( Bindable<T> bindable, DataContext dataContext, Function1<T, Object> rowTransform ) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                return Linq4j.transform( bindable.bind( dataContext ).enumerator(), rowTransform );
            }
        };
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
                : CursorFactory.deduce( getFields(), getResultClass() );

        return cursorFactory;
    }


    public Bindable<PolyValue[]> getBindable() {
        if ( Kind.DDL.contains( kind ) ) {
            return dataContext -> Linq4j.singletonEnumerable( new PolyInteger[]{ PolyInteger.of( 1 ) } );
        }

        if ( bindable != null ) {
            return bindable;
        }
        bindable = preparedResult.getBindable( getCursorFactory() );
        return bindable;
    }


    public boolean hasMoreRows() {
        if ( iterator == null ) {
            throw new GenericRuntimeException( "Implementation was not opened" );
        }
        return iterator.hasNext();
    }


    public List<ColumnMetaData> getFields() {
        if ( fields != null ) {
            return fields;
        }

        final AlgDataType x = switch ( kind ) {
            case INSERT, DELETE, UPDATE, EXPLAIN ->
                // FIXME: getValidatedNodeType is wrong for DML
                    AlgOptUtil.createDmlRowType( kind, statement.getTransaction().getTypeFactory() );
            default -> tupleType;
        };
        final List<ColumnMetaData> columns = QueryProcessorHelpers.getColumnMetaDataList(
                statement.getTransaction().getTypeFactory(),
                x,
                QueryProcessorHelpers.makeStruct( statement.getTransaction().getTypeFactory(), x ),
                preparedResult.getFieldOrigins() );

        this.fields = columns;
        return columns;

    }


    public ResultIterator execute( Statement statement, int batch ) {
        return execute( statement, batch, false, false, false );
    }


    public ResultIterator execute( Statement statement, int batch, boolean isAnalyzed, boolean isTimed, boolean isIndex ) {
        return new ResultIterator(
                createIterator( getBindable(), statement, isAnalyzed ),
                statement,
                batch,
                isTimed,
                isIndex,
                isAnalyzed,
                tupleType,
                executionTimeMonitor,
                this );
    }


    private Iterator<PolyValue[]> createIterator( Bindable<PolyValue[]> bindable, Statement statement, boolean isAnalyzed ) {
        if ( iterator != null ) {
            return this.iterator;
        }

        if ( isAnalyzed ) {
            statement.getOverviewDuration().start( "Execution" );
        }
        final Enumerable<PolyValue[]> enumerable = enumerable( bindable, statement.getDataContext() );

        if ( isAnalyzed ) {
            statement.getOverviewDuration().stop( "Execution" );
        }

        this.iterator = enumerable.iterator();

        return this.iterator;
    }


    public static Meta.StatementType toStatementType( Kind kind ) {
        if ( kind == Kind.SELECT ) {
            return Meta.StatementType.SELECT;
        } else if ( Kind.DDL.contains( kind ) ) {
            return Meta.StatementType.OTHER_DDL;
        } else if ( Kind.DML.contains( kind ) ) {
            return Meta.StatementType.IS_DML;
        }

        throw new GenericRuntimeException( "Illegal statement type: " + kind.name() );
    }


    public StatementType getStatementType() {
        return toStatementType( this.kind );
    }


    public static int getRowsChanged( Statement statement, Iterator<?> iterator, MonitoringType kind ) {
        int rowsChanged = -1;
        Object object;
        while ( iterator.hasNext() ) {
            object = iterator.next();
            int num;
            if ( object != null && object.getClass().isArray() ) {
                Object[] o = (Object[]) object;
                num = ((PolyNumber) o[0]).intValue();
            } else if ( object != null ) {
                num = ((PolyNumber) object).intValue();
            } else {
                throw new GenericRuntimeException( "Result is null" );
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


    public static void addMonitoringInformation( Statement statement, MonitoringType kind, int rowsChanged ) {
        StatementEvent eventData = statement.getMonitoringEvent();
        if ( rowsChanged > 0 ) {
            eventData.setRowCount( rowsChanged );
        }
        if ( MonitoringType.INSERT == kind || MonitoringType.DELETE == kind ) {

            Map<Long, List<PolyValue>> ordered = new HashMap<>();

            List<Map<Long, PolyValue>> values = statement.getDataContext().getParameterValues();
            if ( !values.isEmpty() ) {
                for ( long i = 0; i < statement.getDataContext().getParameterValues().get( 0 ).size(); i++ ) {
                    ordered.put( i, new ArrayList<>() );
                }
            }

            for ( Map<Long, PolyValue> longObjectMap : statement.getDataContext().getParameterValues() ) {
                longObjectMap.forEach( ( k, v ) -> {
                    ordered.get( k ).add( v );
                } );
            }

            eventData.getChangedValues().putAll( ordered );
            if ( MonitoringType.INSERT == kind ) {
                if ( rowsChanged >= 0 ) {
                    eventData.setRowCount( statement.getDataContext().getParameterValues().size() );
                }
            }
        }
    }


}
