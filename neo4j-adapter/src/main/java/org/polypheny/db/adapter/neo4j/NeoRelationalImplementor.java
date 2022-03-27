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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.adapter.neo4j.rules.NeoAlg;
import org.polypheny.db.adapter.neo4j.rules.NeoProject;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.CreateStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.PreparedCreate;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.ReturnStatement;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

public class NeoRelationalImplementor extends AlgShuttleImpl {

    public static final List<Pair<String, String>> ROWCOUNT = List.of( Pair.of( null, "ROWCOUNT" ) );
    private final List<NeoStatement> statements = new ArrayList<>();
    @Getter
    private List<Pair<String, String>> tableCols;

    @Getter
    private AlgOptTable table;

    @Getter
    private NeoEntity entity;


    private ImmutableList<ImmutableList<RexLiteral>> values;

    @Setter
    @Getter
    private AlgNode last;


    public void add( NeoStatement statement ) {
        this.statements.add( statement );
        this.tableCols = statement.tableCols;
    }


    public void setTable( AlgOptTable table ) {
        this.table = table;
        this.entity = (NeoEntity) table.getTable();
    }


    public void addValues( ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        this.values = tuples;
    }


    public boolean hasValues() {
        return this.values != null && !this.values.isEmpty();
    }


    public void addCreate() {
        CreateStatement statement = NeoUtil.CreateStatement.create( values, entity );
        add( statement );
        add( new ReturnStatement( tableCols.size() + " AS ROWCOUNT", ROWCOUNT ) );
    }


    public void addPreparedCreate() {
        if ( last instanceof NeoProject ) {
            PreparedCreate statement = PreparedCreate.createPrepared( ((NeoProject) last), entity );
            add( statement );
            add( new ReturnStatement( tableCols.size() + " AS ROWCOUNT", ROWCOUNT ) );
            return;
        }
        throw new RuntimeException();
    }


    public Expression asExpression() {
        return EnumUtils.constantArrayList( statements
                .stream()
                .map( NeoUtil.NeoStatement::asExpression )
                .collect( Collectors.toList() ), NeoStatement.class );
    }


    public void visitChild( int ordinal, AlgNode input ) {
        assert ordinal == 0;
        ((NeoAlg) input).implement( this );
    }


    public void addReturn( List<RexNode> projects ) {
        NeoStatement statement = NeoStatement.createReturn( projects, entity, tableCols );
        add( statement );
        tableCols = statement.tableCols;
    }

}
