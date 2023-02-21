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
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.schema.Namespace.Schema;


public class LogicalSchema implements Namespace, Schema {

    private final String schemaName;

    @Getter
    private final Map<String, LogicalEntity> tableMap;

    private final Map<String, LogicalEntity> collectionMap;
    @Getter
    private final long id;


    public LogicalSchema( long id, String schemaName, Map<String, LogicalEntity> tableMap, Map<String, LogicalEntity> collectionMap ) {
        this.schemaName = schemaName;
        this.tableMap = tableMap;
        this.collectionMap = collectionMap;
        this.id = id;
    }


    @Override
    public Entity getEntity( String name ) {
        return tableMap.get( name );
    }


    @Override
    public Set<String> getEntityNames() {
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
    public Namespace getSubNamespace( String name ) {
        return null;
    }


    @Override
    public Set<String> getSubNamespaceNames() {
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
    public Namespace snapshot( SchemaVersion version ) {
        return new LogicalSchema( id, schemaName, tableMap, collectionMap );
    }

}
