/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.jdbc;


import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.router.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.schema.PolyphenyDbSchema;


/**
 * The result of preparing a query. It gives the Avatica driver framework the information it needs to create a prepared statement,
 * or to execute a statement directly, without an explicit prepare step.
 *
 * @param <T> element type
 */
public class PolyphenyDbSignature<T> extends Meta.Signature {

    @JsonIgnore
    public final RelDataType rowType;
    @JsonIgnore
    public final PolyphenyDbSchema rootSchema;
    @JsonIgnore
    private final List<RelCollation> collationList;
    private final long maxRowCount;
    private final Bindable<T> bindable;

    @JsonIgnore
    @Getter
    private final Optional<RelRoot> relRoot;

    @Getter
    private final ExecutionTimeMonitor executionTimeMonitor;


    public PolyphenyDbSignature(
            String sql,
            RelRoot optimalRoot,
            List<AvaticaParameter> parameterList,
            Map<String, Object> internalParameters,
            RelDataType rowType,
            List<ColumnMetaData> columns,
            Meta.CursorFactory cursorFactory,
            PolyphenyDbSchema rootSchema,
            List<RelCollation> collationList,
            long maxRowCount,
            Bindable<T> bindable,
            Meta.StatementType statementType,
            ExecutionTimeMonitor executionTimeMonitor ) {
        super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
        this.rowType = rowType;
        this.rootSchema = rootSchema;
        this.collationList = collationList;
        this.maxRowCount = maxRowCount;
        this.bindable = bindable;
        this.executionTimeMonitor = executionTimeMonitor;
        this.relRoot = Optional.of( optimalRoot );
    }

    public PolyphenyDbSignature(
            String sql,
            List<AvaticaParameter> parameterList,
            Map<String, Object> internalParameters,
            RelDataType rowType,
            List<ColumnMetaData> columns,
            Meta.CursorFactory cursorFactory,
            PolyphenyDbSchema rootSchema,
            List<RelCollation> collationList,
            long maxRowCount,
            Bindable<T> bindable,
            Meta.StatementType statementType,
            ExecutionTimeMonitor executionTimeMonitor ) {
        super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
        this.rowType = rowType;
        this.rootSchema = rootSchema;
        this.collationList = collationList;
        this.maxRowCount = maxRowCount;
        this.bindable = bindable;
        this.executionTimeMonitor = executionTimeMonitor;
        this.relRoot = Optional.empty();
    }


    public Enumerable<T> enumerable( DataContext dataContext ) {
        Enumerable<T> enumerable = bindable.bind( dataContext );
        if ( maxRowCount >= 0 ) {
            // Apply limit. In JDBC 0 means "no limit". But for us, -1 means "no limit", and 0 is a valid limit.
            enumerable = EnumerableDefaults.take( enumerable, maxRowCount );
        }
        return enumerable;
    }


    public List<RelCollation> getCollationList() {
        return collationList;
    }
}