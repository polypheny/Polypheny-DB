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

package org.polypheny.db.schema;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.type.AlgProtoDataType;


public class LogicalSchema implements Schema {

    private final String schemaName;
    private final Map<String, LogicalTable> tableMap;


    public LogicalSchema( String schemaName, Map<String, LogicalTable> tableMap ) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
    }


    @Override
    public Table getTable( String name ) {
        return tableMap.get( name );
    }


    @Override
    public Set<String> getTableNames() {
        return tableMap.keySet();
    }


    @Override
    public AlgProtoDataType getType( String name ) {
        return null;
    }


    @Override
    public Set<String> getTypeNames() {
        return ImmutableSet.of();
    }


    @Override
    public Collection<Function> getFunctions( String name ) {
        return ImmutableSet.of();
    }


    @Override
    public Set<String> getFunctionNames() {
        return ImmutableSet.of();
    }


    @Override
    public Schema getSubSchema( String name ) {
        return null;
    }


    @Override
    public Set<String> getSubSchemaNames() {
        return ImmutableSet.of();
    }


    @Override
    public Expression getExpression( SchemaPlus parentSchema, String name ) {
        return Schemas.subSchemaExpression( parentSchema, name, LogicalSchema.class );
    }


    @Override
    public boolean isMutable() {
        return true;
    }


    @Override
    public Schema snapshot( SchemaVersion version ) {
        return new LogicalSchema( schemaName, tableMap );
    }

}
