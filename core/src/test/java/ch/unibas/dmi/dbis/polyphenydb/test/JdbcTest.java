/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableView;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;
import java.util.List;


/**
 * Tests for using Polypheny-DB via JDBC.
 */
public class JdbcTest {

    /**
     * Abstract base class for implementations of {@link ModifiableTable}.
     */
    public abstract static class AbstractModifiableTable extends AbstractTable implements ModifiableTable {

        protected AbstractModifiableTable( String tableName ) {
            super();
        }


        @Override
        public TableModify toModificationRel(
                RelOptCluster cluster,
                RelOptTable table,
                Prepare.CatalogReader catalogReader,
                RelNode child,
                TableModify.Operation operation,
                List<String> updateColumnList,
                List<RexNode> sourceExpressionList,
                boolean flattened ) {
            return LogicalTableModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
        }
    }


    /**
     * Abstract base class for implementations of {@link ModifiableView}.
     */
    public abstract static class AbstractModifiableView extends AbstractTable implements ModifiableView {

        protected AbstractModifiableView() {
            super();
        }
    }



}
