/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface.utils;

import java.util.List;
import org.polypheny.db.protointerface.proto.StatementResult;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.Row;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.StatementStatus;
import org.polypheny.db.protointerface.proto.StatementStatus;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.type.entity.PolyValue;

public class ProtoUtils {


    public static StatementStatus createStatus( ProtoInterfaceStatement protoInterfaceStatement ) {
        return StatementStatus.newBuilder()
                .setStatementId( protoInterfaceStatement.getStatementId() )
                .build();
    }


    public static StatementStatus createStatus( ProtoInterfaceStatement protoInterfaceStatement, StatementResult result ) {
        return StatementStatus.newBuilder()
                .setStatementId( protoInterfaceStatement.getStatementId() )
                .setResult( result )
                .build();
    }

}
