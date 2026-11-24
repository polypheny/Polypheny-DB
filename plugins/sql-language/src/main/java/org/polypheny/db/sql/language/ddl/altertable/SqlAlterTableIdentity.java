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

package org.polypheny.db.sql.language.ddl.altertable;

import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.entity.logical.IdentityColumn.Mode;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.ddl.SqlAlterTable;
import org.polypheny.db.sql.language.ddl.SqlIdentitySpec;
import org.polypheny.db.sql.language.ddl.SqlIdentitySpec.Options;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.Lockable.LockType;

public class SqlAlterTableIdentity extends SqlAlterTable {

    public enum Action { ADD, RESTART, SET_MODE, SET_OPTIONS, DROP }

    private final SqlIdentifier table;
    private final SqlIdentifier column;
    private final Action action;
    private final Mode mode;
    private final Options options;
    private final Long next;
    private final boolean ifExists;


    public SqlAlterTableIdentity( ParserPos pos, SqlIdentifier table, SqlIdentifier column, Action action, Mode mode, Options options, Long next, boolean ifExists ) {
        super( pos );
        this.table = table;
        this.column = column;
        this.action = action;
        this.mode = mode;
        this.options = options;
        this.next = next;
        this.ifExists = ifExists;
    }


    @Override
    public List<Node> getOperandList() {
        return List.of( table, column );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return List.of( table, column );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER TABLE" );
        table.unparse( writer, 0, 0 );
        writer.keyword( "ALTER COLUMN" );
        column.unparse( writer, 0, 0 );
        switch ( action ) {
            case ADD -> {
                writer.keyword( "ADD" );
                new SqlIdentitySpec( mode, options ).unparse( writer );
            }
            case RESTART -> {
                writer.keyword( "RESTART" );
                if ( next != null ) {
                    writer.keyword( "WITH" );
                    writer.literal( next.toString() );
                }
            }
            case SET_MODE -> {
                writer.keyword( "SET GENERATED" );
                SqlIdentitySpec.unparseMode( writer, mode );
            }
            case SET_OPTIONS -> {
                writer.keyword( "SET" );
                options.unparseGroup( writer );
            }
            case DROP -> writer.keyword( ifExists ? "DROP IDENTITY IF EXISTS" : "DROP IDENTITY" );
        }
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        LogicalTable logicalTable = getTableFailOnEmpty( context, table );
        LogicalColumn field = Catalog.snapshot().rel().getColumn( logicalTable.id, column.getSimple() )
                .orElseThrow( () -> new GenericRuntimeException( "Unknown column: " + column ) );
        IdentityColumn existing = field.getIdentity();
        DdlManager manager = DdlManager.getInstance();
        if ( action == Action.ADD ) {
            if ( existing != null ) {
                throw new GenericRuntimeException( "Column already has an identity: " + column );
            }
            if ( field.collectionsType != null ) {
                throw new GenericRuntimeException( "An identity column requires a scalar integer type." );
            }
            IdentityColumn identity = IdentityColumn.create( field.type, mode, options.start(), options.increment(), options.min(), options.max(), Boolean.TRUE.equals( options.cycle() ) );
            manager.setColumnIdentity( logicalTable, field, identity, options.start() == null, statement );
            return;
        }
        if ( existing == null ) {
            if ( action == Action.DROP && ifExists ) {
                return;
            }
            throw new GenericRuntimeException( "Column has no identity: " + column );
        }
        IdentityColumn replacement = switch ( action ) {
            case RESTART -> existing.restart( next );
            case SET_MODE -> existing.withMode( mode );
            case SET_OPTIONS -> options.apply( field.type, existing );
            case DROP -> null;
            default -> throw new IllegalStateException();
        };
        manager.setColumnIdentity( logicalTable, field, replacement, false, statement );
    }


    @Override
    public Map<Lockable, LockType> deriveLockables( Context context, ParsedQueryContext parsedQueryContext ) {
        return getMapOfTableLockable( table, context, LockType.EXCLUSIVE );
    }

}
