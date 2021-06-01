/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail;


import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.vitrivr.cottontail.grpc.CottontailGrpc;


public class CottontailSchema extends AbstractSchema {

    @Getter
    private final CottontailConvention convention;

    private final Map<String, CottontailTable> tableMap;
    private final Map<String, String> physicalToLogicalTableNameMap;

    private final CottontailStore cottontailStore;

    private final String name;

    @Getter
    private final CottontailGrpc.SchemaName cottontailSchema;

    @Getter
    private final CottontailWrapper wrapper;


    private CottontailSchema(
            @NonNull CottontailWrapper wrapper,
            CottontailConvention convention,
            Map<String, CottontailTable> tableMap,
            Map<String, String> physicalToLogicalTableNameMap,
            CottontailStore cottontailStore,
            String name ) {
        this.wrapper = wrapper;
        this.convention = convention;
        this.tableMap = tableMap;
        this.physicalToLogicalTableNameMap = physicalToLogicalTableNameMap;
        this.cottontailStore = cottontailStore;
        this.name = name;
        this.cottontailSchema = CottontailGrpc.SchemaName.newBuilder().setName( this.name ).build();
    }


    public CottontailSchema(
            CottontailWrapper wrapper,
            CottontailConvention convention,
            CottontailStore cottontailStore,
            String name ) {
        this.wrapper = wrapper;
        this.convention = convention;
        this.cottontailStore = cottontailStore;
        this.tableMap = new HashMap<>();
        this.physicalToLogicalTableNameMap = new HashMap<>();
        this.name = name;
        this.cottontailSchema = CottontailGrpc.SchemaName.newBuilder().setName( "cottontail" ).build();
    }


    public static CottontailSchema create(
            SchemaPlus parentSchema,
            String name,
            CottontailWrapper wrapper,
            CottontailStore cottontailStore
    ) {
        final Expression expression = Schemas.subSchemaExpression( parentSchema, name, CottontailSchema.class );
        final CottontailConvention convention = CottontailConvention.of( name, expression );
        return new CottontailSchema( wrapper, convention, cottontailStore, name );
    }


    public void registerStore( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( this.cottontailStore );
    }


    @Override
    public boolean isMutable() {
        return true;
    }


    @Override
    public Schema snapshot( SchemaVersion version ) {
        return new CottontailSchema(
                this.wrapper,
                this.convention,
                this.tableMap,
                this.physicalToLogicalTableNameMap,
                this.cottontailStore,
                this.name );
    }


    @Override
    public Expression getExpression( SchemaPlus parentSchema, String name ) {
        return Schemas.subSchemaExpression( parentSchema, name, CottontailSchema.class );
    }


    @Override
    protected Map<String, Table> getTableMap() {
        return ImmutableMap.copyOf( this.tableMap );
    }

}
