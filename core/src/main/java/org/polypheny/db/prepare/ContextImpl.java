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

package org.polypheny.db.prepare;


import java.util.List;
import java.util.Properties;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.config.PolyphenyDbConnectionConfigImpl;
import org.polypheny.db.transaction.Statement;

@Getter
@Value
public class ContextImpl implements Context {

    @NonFinal
    Snapshot snapshot;
    public JavaTypeFactory typeFactory;
    public DataContext dataContext;
    public Statement statement;
    public String defaultNamespaceName;


    public ContextImpl( Snapshot snapshot, DataContext dataContext, String defaultNamespaceName, Statement statement ) {
        this.snapshot = snapshot;
        this.typeFactory = dataContext.getTypeFactory();
        this.dataContext = dataContext;
        this.defaultNamespaceName = defaultNamespaceName;
        this.statement = statement;
    }


    @Override
    public List<String> getObjectPath() {
        return null;
    }


    @Override
    public void updateSnapshot() {
        this.snapshot = Catalog.snapshot();
    }


    @Override
    public PolyphenyDbConnectionConfig config() {
        return new PolyphenyDbConnectionConfigImpl( new Properties() );
    }

}
