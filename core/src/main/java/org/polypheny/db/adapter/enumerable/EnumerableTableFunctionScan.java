/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableFunctionScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelColumnMapping;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.QueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.TableFunctionImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlUserDefinedTableFunction;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;


/**
 * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.TableFunctionScan} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableTableFunctionScan extends TableFunctionScan implements EnumerableRel {

    public EnumerableTableFunctionScan( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, Type elementType, RelDataType rowType, RexNode call, Set<RelColumnMapping> columnMappings ) {
        super( cluster, traits, inputs, call, elementType, rowType, columnMappings );
    }


    @Override
    public EnumerableTableFunctionScan copy( RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings ) {
        return new EnumerableTableFunctionScan( getCluster(), traitSet, inputs, elementType, rowType, rexCall, columnMappings );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        BlockBuilder bb = new BlockBuilder();
        // Non-array user-specified types are not supported yet
        final JavaRowFormat format;
        if ( getElementType() == null ) {
            format = JavaRowFormat.ARRAY;
        } else if ( rowType.getFieldCount() == 1 && isQueryable() ) {
            format = JavaRowFormat.SCALAR;
        } else if ( getElementType() instanceof Class && Object[].class.isAssignableFrom( (Class) getElementType() ) ) {
            format = JavaRowFormat.ARRAY;
        } else {
            format = JavaRowFormat.CUSTOM;
        }
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), format, false );
        RexToLixTranslator t = RexToLixTranslator.forAggregation( (JavaTypeFactory) getCluster().getTypeFactory(), bb, null, implementor.getConformance() );
        t = t.setCorrelates( implementor.allCorrelateVariables );
        bb.add( Expressions.return_( null, t.translate( getCall() ) ) );
        return implementor.result( physType, bb.toBlock() );
    }


    private boolean isQueryable() {
        if ( !(getCall() instanceof RexCall) ) {
            return false;
        }
        final RexCall call = (RexCall) getCall();
        if ( !(call.getOperator() instanceof SqlUserDefinedTableFunction) ) {
            return false;
        }
        final SqlUserDefinedTableFunction udtf = (SqlUserDefinedTableFunction) call.getOperator();
        if ( !(udtf.getFunction() instanceof TableFunctionImpl) ) {
            return false;
        }
        final TableFunctionImpl tableFunction = (TableFunctionImpl) udtf.getFunction();
        final Method method = tableFunction.method;
        return QueryableTable.class.isAssignableFrom( method.getReturnType() );
    }
}

