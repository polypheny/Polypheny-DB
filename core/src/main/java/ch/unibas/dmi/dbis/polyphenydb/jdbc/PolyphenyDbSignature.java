/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;


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
            Meta.StatementType statementType ) {
        super( columns, sql, parameterList, internalParameters, cursorFactory, statementType );
        this.rowType = rowType;
        this.rootSchema = rootSchema;
        this.collationList = collationList;
        this.maxRowCount = maxRowCount;
        this.bindable = bindable;
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