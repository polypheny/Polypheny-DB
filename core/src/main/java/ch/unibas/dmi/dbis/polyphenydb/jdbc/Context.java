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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.SparkHandler;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunner;
import java.util.List;


/**
 * Context for preparing a statement.
 */
public interface Context {

    JavaTypeFactory getTypeFactory();

    /**
     * Returns the root schema
     */
    PolyphenyDbSchema getRootSchema();

    String getDefaultSchemaName();

    List<String> getDefaultSchemaPath();

    PolyphenyDbConnectionConfig config();

    /**
     * Returns the spark handler. Never null.
     */
    SparkHandler spark();

    DataContext getDataContext();

    /**
     * Returns the path of the object being analyzed, or null.
     *
     * The object is being analyzed is typically a view. If it is already being analyzed further up the stack, the view definition can be deduced to be cyclic.
     */
    List<String> getObjectPath();

    /**
     * Gets a runner; it can execute a relational expression.
     */
    RelRunner getRelRunner();


    Transaction getTransaction();


    long getDatabaseId();

    int getCurrentUserId();

    int getDefaultStore();
}
