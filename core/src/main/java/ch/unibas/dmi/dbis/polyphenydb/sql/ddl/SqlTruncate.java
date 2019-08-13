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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDdl;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Parse tree for {@code TRUNCATE TABLE } statement.
 */
public class SqlTruncate extends SqlDdl implements SqlExecutableStatement {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "TRUNCATE", SqlKind.TRUNCATE );

    protected final SqlIdentifier name;


    /**
     * Creates a SqlDropTable.
     */
    public SqlTruncate( SqlParserPos pos, SqlIdentifier name ) {
        super( OPERATOR, pos );
        this.name = name;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getOperator().getName() );
        writer.keyword( "TABLE" );
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        // Check if there is a table with this name
        long schemaId;
        CatalogCombinedTable table;
        try {
            String tableName;
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                tableName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), name.names.get( 0 ) ).id;
                tableName = name.names.get( 1 );
            } else { // TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableName = name.names.get( 0 );
            }
            table = transaction.getCatalog().getCombinedTable( transaction.getCatalog().getTable( schemaId, tableName ).id );
        } catch ( UnknownDatabaseException | UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.schemaNotFound( name.toString() ) );
        } catch ( UnknownTableException e ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableNotFound( name.toString() ) );
        }

        //  Execute truncate on all placements
        table.getPlacements().forEach( catalogDataPlacement -> {
            StoreManager.getInstance().getStore( catalogDataPlacement.storeId ).truncate( transaction, table );
        } );
    }
}

