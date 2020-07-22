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
 */

package org.polypheny.db.statistic;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


/**
 * Stores all available statistics and updates INSERTs dynamically
 * DELETEs and UPDATEs should wait to be reprocessed
 */
@Slf4j
public class StatisticsManager<T extends Comparable<T>> {

    private static volatile StatisticsManager<?> instance = null;

    @Getter
    public volatile ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn<T>>>> statisticSchemaMap;

    private StatisticQueryProcessor sqlQueryInterface;

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private int buffer = RuntimeConfig.STATISTIC_BUFFER.getInteger();

    @Setter
    @Getter
    private String revalId = null;


    private StatisticsManager() {
        this.statisticSchemaMap = new ConcurrentHashMap<>();
        displayInformation();
        registerTaskTracking();
        registerIsFullTracking();
    }


    /**
     * Registers if on configChange statistics are tracked and displayed or not
     */
    private void registerTaskTracking() {
        TrackingListener listener = new TrackingListener();
        RuntimeConfig.PASSIVE_TRACKING.addObserver( listener );
        RuntimeConfig.DYNAMIC_QUERYING.addObserver( listener );
    }


    /**
     * Registers the isFull reevaluation on config change
     */
    private void registerIsFullTracking() {
        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                buffer = c.getInt();
                resetAllIsFull();
            }


            @Override
            public void restart( Config c ) {
                buffer = c.getInt();
                resetAllIsFull();
            }
        };
        RuntimeConfig.STATISTIC_BUFFER.addObserver( listener );
    }


    public static StatisticsManager<?> getInstance() {
        // To ensure only one instance is created
        synchronized ( StatisticsManager.class ) {
            if ( instance == null ) {
                instance = new StatisticsManager<>();
            }
        }
        return instance;
    }


    /**
     * Gets the specific statisticColumn if it exists in the tracked columns
     * else null
     *
     * @return the statisticColumn which matches the criteria
     */
    private StatisticColumn<T> getColumn( String schema, String table, String column ) {
        if ( this.statisticSchemaMap.containsKey( schema ) ) {
            if ( this.statisticSchemaMap.get( schema ).containsKey( table ) ) {
                if ( this.statisticSchemaMap.get( schema ).get( table ).containsKey( column ) ) {
                    return this.statisticSchemaMap.get( schema ).get( table ).get( column );
                }
            }
        }
        return null;
    }


    /**
     * Adds a new column to the tracked columns and sorts it correctly
     *
     * @param qualifiedColumn column name
     * @param type the type of the new column
     */
    private void addColumn( String qualifiedColumn, PolyType type ) {
        String[] splits = QueryColumn.getSplitColumn( qualifiedColumn );
        if ( type.getFamily() == PolyTypeFamily.NUMERIC ) {
            put( splits[0], splits[1], splits[2], new NumericalStatisticColumn<>( splits, type ) );
        } else if ( type.getFamily() == PolyTypeFamily.CHARACTER ) {
            put( splits[0], splits[1], splits[2], new AlphabeticStatisticColumn<>( splits, type ) );
        }
    }


    /**
     * Reset all statistics and reevaluate them
     */
    private void reevaluateAllStatistics() {
        if ( this.sqlQueryInterface == null ) {
            return;
        }
        log.debug( "Resetting StatisticManager." );
        ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn<T>>>> statisticSchemaMapCopy = new ConcurrentHashMap<>();

        for ( QueryColumn column : this.sqlQueryInterface.getAllColumns() ) {
            StatisticColumn<T> col = reevaluateColumn( column );
            if ( col != null ) {
                put( statisticSchemaMapCopy, column.getSchema(), column.getTable(), column.getName(), col );
            }

        }
        replaceStatistics( statisticSchemaMapCopy );
        log.debug( "Finished resetting StatisticManager." );
    }


    private void resetAllIsFull() {
        this.statisticSchemaMap.values().forEach( s -> s.values().forEach( t -> t.values().forEach( c -> {
            assignUnique( c, this.getUniqueValues( c.getQualifiedTableName(), c.getQualifiedColumnName() ) );
        } ) ) );
    }


    /**
     * Gets a columns of a table and reevaluates them
     *
     * @param qualifiedTable table name
     */
    private void reevaluateTable( String qualifiedTable ) {
        if ( this.sqlQueryInterface == null ) {
            return;
        }

        String[] splits = qualifiedTable.replace( "\"", "" ).split( "\\." );
        if ( splits.length != 2 ) {
            return;
        }
        deleteTable( splits[0], splits[1] );
        List<QueryColumn> res = this.sqlQueryInterface.getAllColumns( splits[0], splits[1] );

        for ( QueryColumn column : res ) {
            StatisticColumn<T> col = reevaluateColumn( column );
            if ( col != null ) {
                put( column.getSchema(), column.getTable(), column.getName(), col );
            }

        }

    }


    private void deleteTable( String schema, String table ) {
        if ( this.statisticSchemaMap.get( schema ) != null ) {
            this.statisticSchemaMap.get( schema ).remove( table );
        }
    }


    /**
     * replace the the tracked statistics with other statistics
     */
    private synchronized void replaceStatistics( ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn<T>>>> map ) {
        this.statisticSchemaMap = new ConcurrentHashMap<>( map );
    }


    /**
     * Method to sort a column into the different kinds of column types and hands it to the specific reevaluation
     */
    private StatisticColumn<T> reevaluateColumn( QueryColumn column ) {
        if ( !this.sqlQueryInterface.hasData( column.getSchema(), column.getTable(), column.getName() ) ) {
            return null;
        }
        if ( column.getType().getFamily() == PolyTypeFamily.NUMERIC ) {
            return this.reevaluateNumericalColumn( column );
        } else if ( column.getType().getFamily() == PolyTypeFamily.CHARACTER ) {
            return this.reevaluateAlphabeticalColumn( column );
        }
        return null;
    }


    /**
     * Reevaluates a numerical column, with the configured statistics
     */
    private StatisticColumn<T> reevaluateNumericalColumn( QueryColumn column ) {
        StatisticQueryColumn min = this.getAggregateColumn( column, "MIN" );
        StatisticQueryColumn max = this.getAggregateColumn( column, "MAX" );
        Integer count = this.getCount( column );
        NumericalStatisticColumn<T> statisticColumn = new NumericalStatisticColumn<>( QueryColumn.getSplitColumn( column.getQualifiedColumnName() ), column.getType() );
        if ( min != null ) {
            //noinspection unchecked
            statisticColumn.setMin( (T) min.getData()[0] );
        }
        if ( max != null ) {
            //noinspection unchecked
            statisticColumn.setMax( (T) max.getData()[0] );
        }

        StatisticQueryColumn unique = this.getUniqueValues( column );
        assignUnique( statisticColumn, unique );

        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Helper method tho assign unique values or set isFull if too much exist
     *
     * @param column the column in which the values should be inserted
     */
    private void assignUnique( StatisticColumn<T> column, StatisticQueryColumn unique ) {
        if ( unique == null || unique.getData() == null ) {
            return;
        }
        if ( unique.getData().length <= this.buffer ) {
            //noinspection unchecked
            column.setUniqueValues( Arrays.asList( (T[]) unique.getData() ) );
        } else {
            column.setFull( true );
        }
    }


    /**
     * Reevaluates an alphabetical column, with the configured statistics
     */
    private StatisticColumn<T> reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatisticQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );

        AlphabeticStatisticColumn<T> statisticColumn = new AlphabeticStatisticColumn<>( QueryColumn.getSplitColumn( column.getQualifiedColumnName() ), column.getType() );
        assignUnique( statisticColumn, unique );
        statisticColumn.setCount( count );

        return statisticColumn;

    }


    /**
     * Places a column at the correct position in the schemaMap
     *
     * @param map which schemaMap should be used
     * @param statisticColumn the Column with its statistics
     */
    private void put( ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn<T>>>> map, String schema, String table, String column, StatisticColumn<T> statisticColumn ) {
        if ( !map.containsKey( schema ) ) {
            map.put( schema, new HashMap<>() );
        }

        if ( !map.get( schema ).containsKey( table ) ) {
            map.get( schema ).put( table, new HashMap<>() );
        }
        map.get( schema ).get( table ).put( column, statisticColumn );

    }


    private void put( String schema, String table, String column, StatisticColumn<T> statisticColumn ) {
        put( this.statisticSchemaMap, schema, table, column, statisticColumn );

    }


    /**
     * Method to get a generic Aggregate Stat
     * null if no result is available
     *
     * @return a StatQueryColumn which contains the requested value
     */
    private StatisticQueryColumn getAggregateColumn( QueryColumn column, String aggregate ) {
        return getAggregateColumn( column.getSchema(), column.getTable(), column.getName(), aggregate );
    }


    /**
     * Queries the database with a aggregate query
     *
     * @param aggregate the aggregate function to us
     */
    private StatisticQueryColumn getAggregateColumn( String schema, String table, String column, String aggregate ) {
        String query = "SELECT " + aggregate + " (" + StatisticQueryProcessor.buildQualifiedName( schema, table, column ) + ") FROM " + StatisticQueryProcessor.buildQualifiedName( schema, table ) + getStatQueryLimit();
        return this.sqlQueryInterface.selectOneStat( query );
    }


    /**
     * Gets the configured amount + 1 of unique values per column
     *
     * @return the unique values
     */
    private StatisticQueryColumn getUniqueValues( QueryColumn column ) {
        String qualifiedTableName = StatisticQueryProcessor.buildQualifiedName( column.getSchema(), column.getTable() );
        String qualifiedColumnName = StatisticQueryProcessor.buildQualifiedName( column.getSchema(), column.getTable(), column.getName() );
        return getUniqueValues( qualifiedTableName, qualifiedColumnName );
    }


    private StatisticQueryColumn getUniqueValues( String qualifiedTableName, String qualifiedColumnName ) {
        String query = "SELECT " + qualifiedColumnName + " FROM " + qualifiedTableName + " GROUP BY " + qualifiedColumnName + getStatQueryLimit( 1 );
        return this.sqlQueryInterface.selectOneStat( query );
    }


    /**
     * Gets the amount of entries for a column
     */
    private Integer getCount( QueryColumn column ) {
        String tableName = StatisticQueryProcessor.buildQualifiedName( column.getSchema(), column.getTable() );
        String columnName = StatisticQueryProcessor.buildQualifiedName( column.getSchema(), column.getTable(), column.getName() );
        String query = "SELECT COUNT(" + columnName + ") FROM " + tableName;
        StatisticQueryColumn res = this.sqlQueryInterface.selectOneStat( query );

        if ( res != null && res.getData() != null && res.getData().length != 0 ) {
            try {
                return Integer.parseInt( res.getData()[0] );
            } catch ( NumberFormatException e ) {
                log.error( "Count could not be parsed for column {}.", column.getQualifiedColumnName(), e );
            }
        }
        return 0;
    }


    private String getStatQueryLimit() {
        return getStatQueryLimit( 0 );
    }


    private String getStatQueryLimit( int add ) {
        return " LIMIT " + (RuntimeConfig.STATISTIC_BUFFER.getInteger() + add);
    }


    public void setSqlQueryInterface( StatisticQueryProcessor statisticQueryProcessor ) {
        this.sqlQueryInterface = statisticQueryProcessor;

        this.asyncReevaluateAllStatistics();

    }


    /**
     * Configures and registers the statistics InformationPage for the frontend
     */
    public void displayInformation() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Statistics" );
        im.addPage( page );

        InformationGroup contentGroup = new InformationGroup( page, "Column Statistic Status" );
        im.addGroup( contentGroup );

        InformationTable statisticsInformation = new InformationTable( contentGroup, Arrays.asList( "Column Name", "Type", "Count" ) );

        im.registerInformation( statisticsInformation );

        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
                    statisticsInformation.reset();
                    statisticSchemaMap.values().forEach( schema -> schema.values().forEach( table -> table.forEach( ( k, v ) -> {
                        statisticsInformation.addRow( v.getQualifiedColumnName(), v.getType().name(), v.getCount() );
                    } ) ) );
                },
                "Reset the Statistic InformationPage for the dynamicQuerying",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS
        );

        InformationGroup alphabeticalGroup = new InformationGroup( page, "Alphabetical Statistics" );
        im.addGroup( alphabeticalGroup );

        InformationGroup numericalGroup = new InformationGroup( page, "Numerical Statistics" );
        im.addGroup( numericalGroup );

        InformationTable numericalInformation = new InformationTable( numericalGroup, Arrays.asList( "Column Name", "Min", "Max" ) );

        InformationTable alphabeticalInformation = new InformationTable( alphabeticalGroup, Arrays.asList( "Column Name", "Unique Values" ) );

        im.registerInformation( numericalInformation );
        im.registerInformation( alphabeticalInformation );

        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
                    numericalInformation.reset();
                    alphabeticalInformation.reset();
                    statisticSchemaMap.values().forEach( schema -> schema.values().forEach( table -> table.forEach( ( k, v ) -> {
                        if ( v instanceof NumericalStatisticColumn ) {

                            if ( ((NumericalStatisticColumn<T>) v).getMin() != null && ((NumericalStatisticColumn<T>) v).getMax() != null ) {
                                numericalInformation.addRow( v.getQualifiedColumnName(), ((NumericalStatisticColumn<T>) v).getMin().toString(), ((NumericalStatisticColumn<T>) v).getMax().toString() );
                            } else {
                                numericalInformation.addRow( v.getQualifiedColumnName(), "❌", "❌" );
                            }

                        } else {
                            String values = v.getUniqueValues().toString();
                            if ( !v.isFull ) {
                                alphabeticalInformation.addRow( v.getQualifiedColumnName(), values );
                            } else {
                                alphabeticalInformation.addRow( v.getQualifiedColumnName(), "is Full" );
                            }

                        }

                    } ) ) );

                }, "Reset Min Max for all numericalColumns.",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS );

    }


    public void asyncReevaluateAllStatistics() {
        threadPool.execute( this::reevaluateAllStatistics );
    }


    /**
     * Reevaluates all tables which received changes impacting their statistic data
     *
     * @param changedTables all tables which got changed in a transaction
     */
    public void apply( List<String> changedTables ) {
        threadPool.execute( () -> changedTables.forEach( this::reevaluateTable ) );
    }


    public void applyTable( String changedQualifiedTable ) {
        this.reevaluateTable( changedQualifiedTable );
    }


    /**
     * This class reevaluates if background tracking should be stopped or restarted depending on the state of the ConfigManager
     */
    class TrackingListener implements Config.ConfigListener {

        @Override
        public void onConfigChange( Config c ) {
            registerTrackingToggle();
        }


        @Override
        public void restart( Config c ) {
            registerTrackingToggle();
        }


        private void registerTrackingToggle() {
            String id = StatisticsManager.getInstance().getRevalId();
            if ( id == null && RuntimeConfig.DYNAMIC_QUERYING.getBoolean() && RuntimeConfig.PASSIVE_TRACKING.getBoolean() ) {
                String revalId = BackgroundTaskManager.INSTANCE.registerTask(
                        StatisticsManager.this::asyncReevaluateAllStatistics,
                        "Reevaluate StatisticsManager.",
                        TaskPriority.LOW,
                        (TaskSchedulingType) RuntimeConfig.STATISTIC_RATE.getEnum() );
                setRevalId( revalId );
            } else if ( id != null && (!RuntimeConfig.PASSIVE_TRACKING.getBoolean() || !RuntimeConfig.DYNAMIC_QUERYING.getBoolean()) ) {
                BackgroundTaskManager.INSTANCE.removeBackgroundTask( getRevalId() );
                setRevalId( null );
            }
        }

    }

}
