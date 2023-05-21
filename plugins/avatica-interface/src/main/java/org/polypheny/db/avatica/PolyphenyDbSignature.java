/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.avatica;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.type.entity.PolyValue;


/**
 * The result of preparing a query. It gives the Avatica driver framework the information it needs to create a prepared statement,
 * or to execute a statement directly, without an explicit prepare step.
 *
 * @param <T> element type
 */
@Getter
public class PolyphenyDbSignature<T> extends Meta.Signature {

    @JsonIgnore
    public final AlgDataType rowType;
    @JsonIgnore
    public final PolyphenyDbSchema rootSchema;
    @JsonIgnore
    private final List<AlgCollation> collationList;
    private final long maxRowCount;
    private final Bindable<T> bindable;
    private final NamespaceType namespaceType;
    private final ExecutionTimeMonitor executionTimeMonitor;


    public PolyphenyDbSignature(
            String sql,
            List<AvaticaParameter> parameterList,
            Map<String, Object> internalParameters,
            AlgDataType rowType,
            List<ColumnMetaData> columns,
            CursorFactory cursorFactory,
            PolyphenyDbSchema rootSchema,
            List<AlgCollation> collationList,
            long maxRowCount,
            Bindable<T> bindable,
            StatementType statementType,
            ExecutionTimeMonitor executionTimeMonitor,
            NamespaceType namespaceType ) {
        super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
        this.rowType = rowType;
        this.rootSchema = rootSchema;
        this.collationList = collationList;
        this.maxRowCount = maxRowCount;
        this.bindable = bindable;
        this.executionTimeMonitor = executionTimeMonitor;
        this.namespaceType = namespaceType;
    }


    public static <T> PolyphenyDbSignature<T> from( PolyImplementation<T> prepareQuery ) {
        final List<AvaticaParameter> parameters = new ArrayList<>();
        if ( prepareQuery.rowType != null ) {
            for ( AlgDataTypeField field : prepareQuery.rowType.getFieldList() ) {
                AlgDataType type = field.getType();
                parameters.add(
                        new AvaticaParameter(
                                false,
                                type.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED ? 0 : type.getPrecision(),
                                0, // This is a workaround for a bug in Avatica with Decimals. There is no need to change the scale //getScale( type ),
                                type.getPolyType().getJdbcOrdinal(),
                                type.getPolyType().getTypeName(),
                                type.getClass().getName(),
                                field.getName() ) );
            }
        }
        return new PolyphenyDbSignature<>(
                "",
                parameters,
                new HashMap<>(),
                prepareQuery.getRowType(),
                prepareQuery.getColumns(),
                prepareQuery.getCursorFactory(),
                null,
                ImmutableList.of(),
                prepareQuery.getMaxRowCount(),
                prepareQuery.getBindable(),
                prepareQuery.getStatementType(),
                prepareQuery.getExecutionTimeMonitor(),
                prepareQuery.getNamespaceType()
        );
    }


    public Enumerable<T> enumerable( DataContext dataContext ) {
        return PolyImplementation.enumerable( bindable, dataContext );
    }


    public Enumerable<T> enumerableTransform( DataContext dataContext ) {
        List<Function<PolyValue, Object>> transform = new ArrayList<>();
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            switch ( field.getType().getPolyType() ) {
                case VARCHAR:
                    transform.add( o -> o.asString().value );
                    break;
                case INTEGER:
                    transform.add( o -> o.asNumber().intValue() );
                    break;
                case BIGINT:
                    transform.add( o -> o.asNumber().bigDecimalValue() );
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        if ( rowType.getFieldCount() > 1 ) {
            return (Enumerable<T>) PolyImplementation.enumerable( bindable, dataContext, row -> {
                Object[] objects = (Object[]) row;
                for ( int i = 0, rowLength = objects.length; i < rowLength; i++ ) {
                    objects[i] = transform.get( i ).apply( (PolyValue) objects[i] );
                }
                return objects;
            } );
        }
        return (Enumerable<T>) PolyImplementation.enumerable( bindable, dataContext, row -> transform.get( 0 ).apply( (PolyValue) row ) );

    }

}