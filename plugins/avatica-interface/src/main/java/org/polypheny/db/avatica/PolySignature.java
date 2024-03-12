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

package org.polypheny.db.avatica;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.type.entity.PolyValue;


/**
 * The result of preparing a query. It gives the Avatica driver framework the information it needs to create a prepared statement,
 * or to execute a statement directly, without an explicit prepare step.
 */
@Getter
public class PolySignature extends Meta.Signature {

    @JsonIgnore
    public final AlgDataType rowType;
    @JsonIgnore
    private final List<AlgCollation> collationList;
    private final long maxRowCount;
    private final Bindable<PolyValue[]> bindable;
    private final DataModel dataModel;
    private final ExecutionTimeMonitor executionTimeMonitor;


    public PolySignature(
            String sql,
            List<AvaticaParameter> parameterList,
            Map<String, Object> internalParameters,
            AlgDataType rowType,
            List<ColumnMetaData> columns,
            CursorFactory cursorFactory,
            List<AlgCollation> collationList,
            long maxRowCount,
            Bindable<PolyValue[]> bindable,
            StatementType statementType,
            ExecutionTimeMonitor executionTimeMonitor,
            DataModel dataModel ) {
        super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
        this.rowType = rowType;
        this.collationList = collationList;
        this.maxRowCount = maxRowCount;
        this.bindable = bindable;
        this.executionTimeMonitor = executionTimeMonitor;
        this.dataModel = dataModel;
    }


    public static PolySignature from( ImplementationContext prepareQuery ) {
        final List<AvaticaParameter> parameters = new ArrayList<>();
        if ( prepareQuery.getImplementation() == null ) {
            if ( prepareQuery.getException().isPresent() ) {
                throw new GenericRuntimeException( prepareQuery.getException().get() );
            }
            return fromError( prepareQuery );
        }
        PolyImplementation implementation = prepareQuery.getImplementation();
        if ( implementation.tupleType != null ) {
            for ( AlgDataTypeField field : prepareQuery.getImplementation().tupleType.getFields() ) {
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
        return new PolySignature(
                "",
                parameters,
                new HashMap<>(),
                implementation.getTupleType(),
                implementation.getFields(),
                implementation.getCursorFactory(),
                ImmutableList.of(),
                implementation.getMaxRowCount(),
                implementation.getBindable(),
                implementation.getStatementType(),
                implementation.getExecutionTimeMonitor(),
                implementation.getDataModel()
        );
    }


    private static PolySignature fromError( ImplementationContext prepareQuery ) {
        return new PolySignature(
                "",
                new ArrayList<>(),
                new HashMap<>(),
                null,
                new ArrayList<>(),
                null,
                ImmutableList.of(),
                -1,
                null,
                PolyImplementation.toStatementType( prepareQuery.getQuery().getQueryNode().map( Node::getKind ).orElse( Kind.SELECT ) ),
                null,
                prepareQuery.getQuery().getLanguage().dataModel()
        );
    }


    public Enumerable<PolyValue[]> enumerable( DataContext dataContext ) {
        return PolyImplementation.enumerable( bindable, dataContext );
    }

}