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

package org.polypheny.db.workflow.dag.activities;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

// TODO: write test to ensure at most 1 output was specified
public interface Fusable extends Activity {

    /**
     * Whether this activity instance can be fused, given the input tuple types and setting values.
     * If no final decision can be made yet, an empty optional is returned.
     * If this method is overridden, it is required to also provide a custom execute implementation.
     * This is necessary, as it will be used in the case that the activity cannot be fused.
     *
     * @param inTypes preview of the input types. For inactive edges, the type is {@link org.polypheny.db.workflow.dag.activities.TypePreview.InactiveType} (important for non-default DataStateMergers).
     * @param settings preview of the settings
     * @return an Optional containing the final decision whether this activity can be fused, or an empty Optional if it cannot be stated at this point.
     */
    default Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return Optional.of( true );
    }


    @Override
    default void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        assert canFuse(
                inputs.stream().map( r -> TypePreview.ofType( r.getTupleType() ) ).toList(),
                SettingsPreview.of( settings )
        ).orElseThrow() : "Cannot use the default execute implementation of Fusable if canFuse returns false.";

        ctx.logInfo( "Relying on the fusable implementation of " + getClass().getSimpleName() + " to execute the activity." );

        // Imitates the fusion executor
        Transaction transaction = ctx.getTransaction();
        Statement statement = transaction.createStatement();
        AlgCluster cluster = AlgCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ),
                null,
                statement.getDataContext().getSnapshot() );

        List<AlgNode> inNodes = inputs.stream().map( reader -> reader.getAlgNode( cluster ) ).toList();
        long estimatedTupleCount = estimateTupleCount(
                inNodes.stream().map( AlgNode::getTupleType ).toList(),
                settings,
                inputs.stream().map( CheckpointReader::getTupleCount ).toList(),
                () -> transaction );

        FuseExecutionContext fuseCtx = (ExecutionContextImpl) ctx;
        AlgRoot root = AlgRoot.of( fuse( inNodes, settings, cluster, fuseCtx ), Kind.SELECT );

        if ( !QueryUtils.validateAlg( root, false, null ) ) {
            ctx.throwException( "The fused AlgNode tree may not perform data manipulation" );
        }

        ExecutedContext executedContext = QueryUtils.executeAlgRoot( root, statement );
        if ( executedContext.getException().isPresent() ) {
            executedContext.getException().get().printStackTrace();
            ctx.throwException( "An error occurred while executing the fused activity: " + executedContext.getException().get().getMessage() );
        }

        long countDelta = Math.max( estimatedTupleCount / 100, 1 );
        long count = 0;
        Iterator<PolyValue[]> iterator = executedContext.getIterator().getIterator();
        CheckpointWriter writer = ctx.createWriter( 0, root.validatedRowType );
        try {
            while ( iterator.hasNext() ) {
                writer.write( Arrays.asList( iterator.next() ) );
                count++;
                if ( estimatedTupleCount > 0 && count % countDelta == 0 ) {
                    ctx.updateProgress( (double) count / estimatedTupleCount );
                }
                ctx.checkInterrupted();
            }
        } catch ( Exception e ) {
            e.printStackTrace();
            ctx.throwException( e );
        } finally {
            executedContext.getIterator().close();
        }
    }

    /**
     * Return an AlgNode representing the new root of a logical query plan.
     *
     * @param inputs A list of logical input AlgNodes. For relational inputs, the first column contains the primary key. Make sure to remove unnecessary primary key columns, for instance when joining 2 tables. For inactive edges, the AlgNode is null (important for non-default DataStateMergers).
     * @param settings The resolved settings
     * @param cluster the AlgCluster that is used for the construction of the query plan
     * @param ctx ExecutionContext to be used for logging
     * @return The created logical AlgNode. In case of a relational result, its tuple type has the first column reserved for the primary key. It can be left empty.
     */
    AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception;

}
