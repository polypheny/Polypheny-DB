/*
 * Copyright 2019-2022 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Properties;
import lombok.Getter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.config.PolyphenyDbConnectionConfigImpl;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.transaction.Statement;


public class ContextImpl implements Context {

    @Getter
    private final PolyphenyDbSchema rootSchema;
    @Getter
    private final JavaTypeFactory typeFactory;
    @Getter
    private final DataContext dataContext;
    @Getter
    private final Statement statement;
    @Getter
    private final String defaultSchemaName;
    @Getter
    private final long databaseId;
    @Getter
    private final int currentUserId;


    public ContextImpl( PolyphenyDbSchema rootSchema, DataContext dataContext, String defaultSchemaName, long databaseId, int currentUserId, Statement statement ) {
        this.rootSchema = rootSchema;
        this.typeFactory = dataContext.getTypeFactory();
        this.dataContext = dataContext;
        this.defaultSchemaName = defaultSchemaName;
        this.statement = statement;
        this.currentUserId = currentUserId;
        this.databaseId = databaseId;
    }


    @Override
    public List<String> getDefaultSchemaPath() {
        return defaultSchemaName == null
                ? ImmutableList.of()
                : ImmutableList.of( defaultSchemaName );
    }


    @Override
    public List<String> getObjectPath() {
        return null;
    }


    @Override
    public PolyphenyDbConnectionConfig config() {
        return new PolyphenyDbConnectionConfigImpl( new Properties() );
    }

}
