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

package org.polypheny.db.avatica;


import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;


/**
 *
 */
@Slf4j
public class DbmsService implements Service {

    private LocalService delegate;
    private RpcMetadataResponse rpcMetaData;


    public DbmsService( DbmsMeta meta, MetricsSystem metrics ) {
        this.delegate = new LocalService( meta, metrics );
    }


    @Override
    public ResultSetResponse apply( CatalogsRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( CatalogsRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( SchemasRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( SchemasRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( TablesRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( TablesRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( TableTypesRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( TableTypesRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( TypeInfoRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( TypeInfoRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( ColumnsRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( ColumnsRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( PrimaryKeysRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( PrimaryKeysRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( ImportedKeysRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( ImportedKeysRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( ExportedKeysRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( ExportedKeysRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( IndexInfoRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( IndexInfoRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public PrepareResponse apply( PrepareRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( PrepareRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteResponse apply( ExecuteRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( ExecuteRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteResponse apply( PrepareAndExecuteRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( PrepareAndExecuteRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public SyncResultsResponse apply( SyncResultsRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( SyncResultsRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public FetchResponse apply( FetchRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( FetchRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CreateStatementResponse apply( CreateStatementRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( CreateStatementRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CloseStatementResponse apply( CloseStatementRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( CloseStatementRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public OpenConnectionResponse apply( OpenConnectionRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( OpenConnectionRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CloseConnectionResponse apply( CloseConnectionRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( CloseConnectionRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ConnectionSyncResponse apply( ConnectionSyncRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( ConnectionSyncRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public DatabasePropertyResponse apply( DatabasePropertyRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( DatabasePropertyRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CommitResponse apply( CommitRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( CommitRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public RollbackResponse apply( RollbackRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( RollbackRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteBatchResponse apply( PrepareAndExecuteBatchRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( PrepareAndExecuteBatchRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteBatchResponse apply( ExecuteBatchRequest request ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "apply( ExecuteBatchRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public void setRpcMetadata( RpcMetadataResponse metadata ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "setRpcMetadata( RpcMetadataResponse {} )", metadata );
        }
        this.rpcMetaData = metadata;
        delegate.setRpcMetadata( metadata );
    }

}
