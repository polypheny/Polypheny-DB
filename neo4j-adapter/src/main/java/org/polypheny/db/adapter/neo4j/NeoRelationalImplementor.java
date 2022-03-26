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

package org.polypheny.db.adapter.neo4j;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.util.Pair;

public class NeoRelationalImplementor extends AlgShuttleImpl {

    private final List<NeoStatement> statements = new ArrayList<>();
    @Getter
    private Pair<String, String> tableCol;

    @Setter
    @Getter
    private AlgOptTable table;

    @Setter
    @Getter
    private NeoEntity entity;


    public void add( NeoStatement statement ) {
        this.statements.add( statement );
        this.tableCol = statement.tableCol;
    }


    enum StatementType {
        MATCH,
        CREATE,
        WHERE,
        RETURN,
        WITH
    }


    @Getter
    public abstract static class NeoStatement {

        final StatementType type;
        final String query;
        private final Pair<String, String> tableCol;


        protected NeoStatement( StatementType type, String query, Pair<String, String> tableCol ) {
            this.type = type;
            this.query = query;
            this.tableCol = tableCol;
        }


        public abstract String build();

    }


    public static class MatchStatement extends NeoStatement {

        public MatchStatement( String query, Pair<String, String> tableCol ) {
            super( StatementType.MATCH, query, tableCol );
        }


        @Override
        public String build() {
            return "MATCH " + query;
        }

    }


    public static class WhereStatement extends NeoStatement {

        public WhereStatement( String query, Pair<String, String> tableCol ) {
            super( StatementType.WHERE, query, tableCol );
        }


        @Override
        public String build() {
            return "WHERE " + query;
        }

    }


    public static class CreateStatement extends NeoStatement {

        protected CreateStatement( String query, Pair<String, String> tableCol ) {
            super( StatementType.CREATE, query, tableCol );
        }


        @Override
        public String build() {
            return "CREATE " + query;
        }

    }


    public static class ReturnStatement extends NeoStatement {

        protected ReturnStatement( String query, Pair<String, String> tableCol ) {
            super( StatementType.RETURN, query, tableCol );
        }


        @Override
        public String build() {
            return "RETURN " + query;
        }

    }

}
