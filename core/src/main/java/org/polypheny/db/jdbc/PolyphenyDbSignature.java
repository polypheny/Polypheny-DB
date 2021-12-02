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
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.schema.PolyphenyDbSchema;


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
    private final SchemaType schemaType;
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
            SchemaType schemaType ) {
        super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
        this.rowType = rowType;
        this.rootSchema = rootSchema;
        this.collationList = collationList;
        this.maxRowCount = maxRowCount;
        this.bindable = bindable;
        this.executionTimeMonitor = executionTimeMonitor;
        this.schemaType = schemaType;
    }


    public Enumerable<T> enumerable( DataContext dataContext ) {
        Enumerable<T> enumerable = bindable.bind( dataContext );
        if ( maxRowCount >= 0 ) {
            // Apply limit. In JDBC 0 means "no limit". But for us, -1 means "no limit", and 0 is a valid limit.
            enumerable = EnumerableDefaults.take( enumerable, maxRowCount );
        }
        return enumerable;
    }
}