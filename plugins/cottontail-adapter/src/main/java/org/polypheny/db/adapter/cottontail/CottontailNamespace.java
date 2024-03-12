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

package org.polypheny.db.adapter.cottontail;


import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.schema.Namespace;
import org.vitrivr.cottontail.grpc.CottontailGrpc;


public class CottontailNamespace extends Namespace {

    @Getter
    private final CottontailConvention convention = CottontailConvention.INSTANCE;

    private final Map<String, CottontailEntity> tableMap;
    private final Map<String, String> physicalToLogicalTableNameMap;

    private final CottontailStore cottontailStore;

    private final String name;

    @Getter
    private final CottontailGrpc.SchemaName cottontailSchema;

    @Getter
    private final CottontailWrapper wrapper;


    private CottontailNamespace(
            long id,
            @NonNull CottontailWrapper wrapper,
            Map<String, CottontailEntity> tableMap,
            Map<String, String> physicalToLogicalTableNameMap,
            CottontailStore cottontailStore,
            String name ) {
        super( id, cottontailStore.adapterId );
        this.wrapper = wrapper;
        this.tableMap = tableMap;
        this.physicalToLogicalTableNameMap = physicalToLogicalTableNameMap;
        this.cottontailStore = cottontailStore;
        this.name = name;
        this.cottontailSchema = CottontailGrpc.SchemaName.newBuilder().setName( this.name ).build();
    }


    public CottontailNamespace(
            long id,
            CottontailWrapper wrapper,
            CottontailStore cottontailStore,
            String name ) {
        super( id, cottontailStore.adapterId );
        this.wrapper = wrapper;
        this.cottontailStore = cottontailStore;
        this.tableMap = new HashMap<>();
        this.physicalToLogicalTableNameMap = new HashMap<>();
        this.name = name;
        this.cottontailSchema = CottontailGrpc.SchemaName.newBuilder().setName( "cottontail" ).build();
    }


    public static CottontailNamespace create(
            Long id,
            String name,
            CottontailWrapper wrapper,
            CottontailStore cottontailStore
    ) {
        return new CottontailNamespace( id, wrapper, cottontailStore, name );
    }


    @SuppressWarnings("unused")
    public void registerStore( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( this.cottontailStore );
    }


}
