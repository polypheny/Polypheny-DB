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

package org.polypheny.db.sql.sql.ddl;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.*;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.polypheny.db.util.Static.RESOURCE;

public class SqlExecuteProcedure extends SqlCall implements ExecutableStatement {
    private final SqlIdentifier identifier;

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("EXEC PROCEDURE", Kind.PROCEDURE_EXEC);
    private final List<SqlNode> argumentList;
    private final Map<String, Object> arguments = new HashMap<>();

    public SqlExecuteProcedure(ParserPos pos, SqlIdentifier identifier, List<SqlNode> argumentList) {
        super(pos);
        this.identifier = identifier;
        this.argumentList = argumentList;
        for (SqlNode sqlNode : argumentList) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlNode value = sqlBasicCall.operand(1);
            if(value instanceof SqlNumericLiteral) {
                var numericValue = (SqlNumericLiteral) value;
                BigDecimal bigDecimal = numericValue.bigDecimalValue();
                arguments.put(sqlBasicCall.operand(0).toString(), bigDecimal.longValue());
            } else {
                arguments.put(sqlBasicCall.operand(0).toString(), value.toString());
            }
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXEC");
        writer.keyword("PROCEDURE");
        identifier.unparse(writer, 0, 0);
        if (argumentList.size() > 0) {
            final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
            for (SqlNode sqlNode : argumentList) {
                writer.sep(",");
                writer.print(sqlNode.toString());
            }
            writer.endList(frame);
        }
    }

    @Override
    public List<Node> getOperandList() {
        return List.of(identifier);
    }

    @Override
    public Operator getOperator() {
        return OPERATOR;
    }

    public Object[] getKey() {
        if(identifier.names.isEmpty()) {
            throw new RuntimeException("Can't retrieve key without all required identifier parts: databaseId.schemaId.procedureName");
        }
        String[] fqdn = identifier.names.get(0).split("\\.");
        if(fqdn.length != 3) {
            throw new RuntimeException("Can't retrieve key without all required identifier parts: databaseId.schemaId.procedureName");
        }
        try {
            long databaseId = Catalog.getInstance().getDatabase(fqdn[0].toUpperCase()).id;
            long schemaId = Catalog.getInstance().getSchema(databaseId, fqdn[1]).id;
            String procedureName = fqdn[2];
            return new Object[]{databaseId, schemaId, procedureName};
        } catch (UnknownSchemaException | UnknownDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Context context, Statement statement, QueryParameters parameters) {
        DdlManager instance = DdlManager.getInstance();
        Catalog catalog = Catalog.getInstance();
        long schemaId;
        String procedureName;
        long databaseId = context.getDatabaseId();
        try {
            if (identifier.names.size() == 3) { // DatabaseName.SchemaName.ProcedureName
                schemaId = catalog.getSchema(identifier.names.get(0), identifier.names.get(1)).id;
                procedureName = identifier.names.get(2);
                databaseId = catalog.getDatabase(identifier.names.get(0)).id;
            } else if (identifier.names.size() == 2) { // SchemaName.ProcedureName
                schemaId = catalog.getSchema(databaseId, identifier.names.get(0)).id;
                procedureName = identifier.names.get(1);
            } else { // ProcedureName
                schemaId = catalog.getSchema(context.getDatabaseId(), context.getDefaultSchemaName()).id;
                procedureName = identifier.names.get(0);
            }
        } catch (UnknownDatabaseException e) {
            throw CoreUtil.newContextException(identifier.getPos(), RESOURCE.databaseNotFound(identifier.toString()));
        } catch (UnknownSchemaException e) {
            throw CoreUtil.newContextException(identifier.getPos(), RESOURCE.schemaNotFound(identifier.toString()));
        }

        instance.executeProcedure(statement, databaseId, schemaId, identifier.getSimple(), arguments);
    }

    @Override
    public List<SqlNode> getSqlOperandList() {
        return List.of(identifier);
    }

    @SuppressWarnings("unchecked")
    private List<Pair<SqlNode, SqlNode>> pairs() {
        return Util.pairs(argumentList);
    }
}
