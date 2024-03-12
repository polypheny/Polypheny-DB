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

package org.polypheny.db.adapter.file.algebra;


import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileMethod;
import org.polypheny.db.adapter.file.FileSchema;
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.adapter.file.util.FileUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;


public class FileToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    private final Method enumeratorMethod;
    private final FileSchema fileSchema;


    public FileToEnumerableConverter( AlgCluster cluster, AlgTraitSet traits, AlgNode input, Method enumeratorMethod, FileSchema fileSchema ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
        this.enumeratorMethod = enumeratorMethod;
        this.fileSchema = fileSchema;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new FileToEnumerableConverter( getCluster(), traitSet, sole( inputs ), enumeratorMethod, fileSchema );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        FileConvention convention = (FileConvention) getInput().getConvention();
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );

        FileImplementor fileImplementor = new FileImplementor();
        fileImplementor.visitChild( 0, getInput() );

        List<Expression> columnIds = new ArrayList<>();
        List<Expression> columnTypes = new ArrayList<>();
        for ( PhysicalColumn column : fileImplementor.getFileTable().columns ) {
            columnIds.add( Expressions.constant( column.id, Long.class ) );
            columnTypes.add( Expressions.constant( fileImplementor.getFileTable().getColumnTypeMap().get( column.id ), PolyType.class ) );
        }

        Expression _insertValues = Expressions.constant( null );
        Type resType = Types.of( List.class, Types.of( List.class, PolyValue.class ) );

        if ( fileImplementor.getOperation() != Operation.SELECT ) {
            ArrayList<Expression> rowExpressions = new ArrayList<>();
            for ( Value[] row : fileImplementor.getInsertValues() ) {
                rowExpressions.add( FileUtil.getValuesExpression( Arrays.asList( row ) ) );
            }

            _insertValues = EnumUtils.constantArrayList( rowExpressions, resType );
        }

        Expression _updates;
        if ( fileImplementor.getUpdates() != null ) {
            _updates = EnumUtils.constantArrayList( List.of( FileUtil.getValuesExpression( fileImplementor.getUpdates() ) ), resType );
        } else {
            _updates = Expressions.constant( null );
        }

        Expression conditionExpression;
        if ( fileImplementor.getCondition() != null ) {
            conditionExpression = fileImplementor.getCondition().getExpression();
        } else {
            conditionExpression = Expressions.constant( null );
        }

        Expression enumerable;
        // SELECT, UPDATE, DELETE
        if ( fileImplementor.getOperation() != Operation.INSERT ) {
            enumerable = list.append(
                    "enumerable",
                    Expressions.call(
                            enumeratorMethod,
                            Expressions.constant( fileImplementor.getOperation() ),
                            Expressions.constant( fileImplementor.getFileTable().getAdapterId() ),
                            Expressions.constant( fileImplementor.getFileTable().allocationId ),
                            DataContext.ROOT,
                            Expressions.constant( fileSchema.getRootDir().getAbsolutePath() ),
                            Expressions.newArrayInit( Long.class, columnIds.toArray( new Expression[0] ) ),
                            fileImplementor.getFileTable().asExpression( FileTranslatableEntity.class ),
                            //.Expressions.newArrayInit( PolyType.class, columnTypes.toArray( new Expression[0] ) ),
                            Expressions.constant( fileImplementor.getFileTable().getPkIds() ),
                            fileImplementor.getProjectExpressions(),
                            conditionExpression,
                            _updates
                    ) );
        } else { //INSERT
            enumerable = list.append(
                    "enumerable",
                    Expressions.call(
                            FileMethod.EXECUTE_MODIFY.method,
                            Expressions.constant( fileImplementor.getOperation() ),
                            Expressions.constant( fileImplementor.getFileTable().getAdapterId() ),
                            Expressions.constant( fileImplementor.getFileTable().allocationId ),
                            DataContext.ROOT,
                            Expressions.constant( fileSchema.getRootDir().getAbsolutePath() ),
                            Expressions.newArrayInit( Long.class, columnIds.toArray( new Expression[0] ) ),
                            fileImplementor.getFileTable().asExpression( FileTranslatableEntity.class ),
                            Expressions.constant( fileImplementor.getFileTable().getPkIds() ),
                            Expressions.constant( fileImplementor.isBatchInsert() ),
                            _insertValues,
                            conditionExpression
                    ) );
        }

        list.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, Blocks.toBlock( list.toBlock() ) );
    }


}
