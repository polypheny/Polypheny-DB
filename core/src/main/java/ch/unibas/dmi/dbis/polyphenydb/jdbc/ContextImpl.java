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
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfigImpl;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.LongSchemaVersion;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Properties;


public class ContextImpl implements Context {

    private final PolyphenyDbSchema mutableRootSchema;
    private final PolyphenyDbSchema rootSchema;
    private final JavaTypeFactory typeFactory;
    private final DataContext dataContext;
    private final String schemaName;


    public ContextImpl( PolyphenyDbSchema rootSchema, DataContext dataContext, String schemaName ) {
        long now = System.currentTimeMillis();
        SchemaVersion schemaVersion = new LongSchemaVersion( now );
        this.mutableRootSchema = rootSchema;
        this.rootSchema = mutableRootSchema.createSnapshot( schemaVersion );
        this.typeFactory = dataContext.getTypeFactory();
        this.dataContext = dataContext;
        this.schemaName = schemaName;
    }


    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }


    public PolyphenyDbSchema getRootSchema() {
        return rootSchema;
    }


    public PolyphenyDbSchema getMutableRootSchema() {
        return mutableRootSchema;
    }


    public List<String> getDefaultSchemaPath() {
        return schemaName == null
                ? ImmutableList.of()
                : ImmutableList.of( schemaName );
    }


    public List<String> getObjectPath() {
        return null;
    }


    public PolyphenyDbConnectionConfig config() {
        return new PolyphenyDbConnectionConfigImpl( new Properties() );
    }


    public DataContext getDataContext() {
        return dataContext;
    }


    public RelRunner getRelRunner() {
        return null;
    }


    public PolyphenyDbPrepare.SparkHandler spark() {
        final boolean enable = RuntimeConfig.SPARK_ENGINE.getBoolean();
        return PolyphenyDbPrepare.Dummy.getSparkHandler( enable );
    }
}
