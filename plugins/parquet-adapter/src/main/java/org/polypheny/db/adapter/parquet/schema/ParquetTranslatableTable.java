package org.polypheny.db.adapter.parquet.schema;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.adapter.parquet.planning.ParquetScan;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Translatable table
 */
public class ParquetTranslatableTable extends ParquetTable implements TranslatableEntity {

    protected ParquetTranslatableTable( long id, Source source, PhysicalTable table, List<PolyType> fieldTypes, int[] fields, ParquetSource parquetSource ) {
        super( id, source, table, fieldTypes, fields, parquetSource );
    }


    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new Enumerator<>() {
                    @Override
                    public PolyValue[] current() {
                        return new PolyValue[0];
                    }
                    @Override
                    public boolean moveNext() {
                        return false;
                    }
                    @Override
                    public void reset() {

                    }
                    @Override
                    public void close() {

                    }
                };
            }
        };
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new ParquetScan( cluster, this, this, fields );
    }

}
