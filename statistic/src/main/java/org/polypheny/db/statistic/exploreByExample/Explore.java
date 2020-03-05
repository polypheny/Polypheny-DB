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

package org.polypheny.db.statistic.exploreByExample;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.statistic.StatisticQueryColumn;
import org.polypheny.db.statistic.StatisticResult;
import org.polypheny.db.statistic.StatisticsManager;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;


@Slf4j
public class Explore {

    private static Explore INSTANCE = null;

    @Getter
    @Setter
    private String[][] classifiedData;

    @Setter
    private String[] columnId;

    @Setter
    private String tableId;

    List<List<String>> allUniqueValues = new ArrayList<>();
    List<List<String>> allUniqueValuesTF = new ArrayList<>();

    String[][] wholeTable;
    String[][] wholeTableRotated;


    private Explore() {

    }


    public synchronized static Explore getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new Explore();
        }
        return INSTANCE;
    }


    public String[][] classificationProcess() throws Exception {
        getStatistics();
        return prepareUserInput();
    }


    public String[][] prepareUserInput() throws Exception {

        String[][] rotated = rotate2dArray( classifiedData );

        return convertToArff( rotated, classifiedData, wholeTableRotated, wholeTable );
    }


    /**
     * Converts Data to "arff format" in order to use Weka for classification
     *  @param rotated rotated userClassification
     * @param userClassification classified data form user
     * @return
     */
    public String[][] convertToArff( String[][] rotated, String[][] userClassification, String[][] wholeTableRotated, String[][] wholeTable ) throws Exception {

        int numInstances = rotated[0].length;
        int dimLength = classifiedData[0].length;
        FastVector atts = new FastVector();
        FastVector attVals;
        FastVector attValsEl[] = new FastVector[dimLength];
        Instances classifiedData;
        Instances allData;
        double[] vals;

        // attributes
        for ( int dim = 0; dim < dimLength; dim++ ) {
            attVals = new FastVector();

            for ( int i = 0; i < allUniqueValues.get( dim ).size(); i++ ) {
                attVals.addElement( allUniqueValues.get( dim ).get( i ) );
            }

            atts.addElement( new Attribute( "attr" + dim, attVals ) );
            attValsEl[dim] = attVals;
        }
        // instances object
        classifiedData = new Instances( "ClassifiedData", atts, 0 );
        allData = new Instances( "allData", atts, 0 );

        // fill data classified
        for ( int obj = 0; obj < numInstances; obj++ ) {
            vals = new double[classifiedData.numAttributes()];
            for ( int dim = 0; dim < dimLength; dim++ ) {
                vals[dim] = attValsEl[dim].indexOf( userClassification[obj][dim] );
            }
            classifiedData.add( new DenseInstance( 1.0, vals ) );
        }

        //fill all data for classification

        for ( int obj = 0; obj < wholeTable[0].length; obj++ ) {
            vals = new double[allData.numAttributes()];
            for ( int dim = 0; dim < wholeTableRotated[0].length; dim++ ) {
                vals[dim] = attValsEl[dim].indexOf( wholeTableRotated[obj][dim] );
            }
            vals[wholeTableRotated[0].length] = Utils.missingValue();
            allData.add( new DenseInstance( 1, vals ) );
        }

        System.out.println( classifiedData );
        System.out.println( allData );

        //saveAsArff( data, "test.arff" );
        return trainData( classifiedData, allData );
        //trainData();
    }


    /**
     * Removes last value from dataset
     */
    private Instances prepareData( Instances allData ) throws Exception {
        String[] options = new String[2];
        options[0] = "-R";
        options[1] = "last";

        Remove remove = new Remove();
        remove.setOptions( options );
        remove.setInputFormat( allData );
        Instances preparedData = Filter.useFilter( allData, remove );

        return preparedData;
    }


    /**
     * Train table with the classified dataset
     *
     * @param classifiedData prepared classifiedData
     */
    public String[][] trainData( Instances classifiedData, Instances unlabeled ) throws Exception {
        //Instances data = getDataSet( "explore-by-example/test.arff" );

        classifiedData.setClassIndex( classifiedData.numAttributes() - 1 );
        J48 tree = new J48();

        String[] options = { "-U" };
        tree.setOptions( options );

        tree.buildClassifier( classifiedData );

        //Instances unlabeled = new Instances(  new BufferedReader( new FileReader("explore-by-example/exploreExample.arff" ) ));

        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );

        Evaluation evaluation = new Evaluation( classifiedData );
        evaluation.evaluateModel( tree, unlabeled );
        System.out.println( evaluation.toSummaryString() );

        Instances labeled = new Instances( unlabeled );

        String[][] labledData = new String[unlabeled.numInstances()][];

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            System.out.println( clsLabel );
            System.out.println( labeled.instance( i ) );
            labeled.instance( i ).setClassValue( clsLabel );
            if ( "true".equals( unlabeled.classAttribute().value( (int) clsLabel ) ) ){
                labledData[i] = Arrays.copyOf(labeled.instance( i ).toString().split( "," ), labeled.instance( i ).toString().split( "," ).length-1);
            }
            System.out.println( labeled.instance( i ) );
            System.out.println( clsLabel + " -> " + unlabeled.classAttribute().value( (int) clsLabel ) );
        }

        saveAsArff( labeled, "labeled-data.arff" );

        return labledData;
    }


    /**
     * gets all UniqueValues for "arff file"
     *
     * gets whole dataset
     */
    private void getStatistics() {
        StatisticsManager<?> stats = StatisticsManager.getInstance();
        List<StatisticQueryColumn> uniqueValues = stats.getAllUniqueValues( Arrays.asList( columnId ), tableId );

        for ( StatisticQueryColumn uniqueValue : uniqueValues ) {
            allUniqueValues.add( Arrays.asList( uniqueValue.getData() ) );
        }

        List<String> trueFalse = new ArrayList<>();
        trueFalse.add( "true" );
        trueFalse.add( "false" );
        allUniqueValues.add( trueFalse );

        StatisticResult statisticResult = stats.getTable( columnId, tableId );
        StatisticQueryColumn[] columns = statisticResult.getColumns();
        wholeTable = new String[columns.length][];
        for ( int i = 0; i < columns.length; i++ ) {
            wholeTable[i] = columns[i].getData();
        }

        wholeTableRotated = rotate2dArray( wholeTable );


    }


    public Instances getDataSet( String fileName ) throws IOException {

        ArffLoader loader = new ArffLoader();
        loader.setFile( new File( fileName ) );

        Instances dataSet = loader.getDataSet();
        dataSet.setClassIndex( dataSet.numAttributes() - 1 );

        return dataSet;
    }


    public void saveAsArff( Instances dataset, String name ) throws IOException {
        ArffSaver saver = new ArffSaver();
        saver.setInstances( dataset );
        saver.setFile( new File( "explore-by-example/" + name ) );
        saver.writeBatch();
    }


    private String[][] rotate2dArray( String[][] data ) {
        int width = data[0].length;
        int height = data.length;

        String[][] rotated = new String[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotated[x][y] = data[y][x];
            }
        }
        return rotated;
    }
}
