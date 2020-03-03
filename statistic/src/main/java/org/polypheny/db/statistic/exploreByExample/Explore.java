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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.statistic.StatisticQueryColumn;
import org.polypheny.db.statistic.StatisticResult;
import org.polypheny.db.statistic.StatisticsManager;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;


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

    List<List<String>> allUniqueValues = new ArrayList<>(  );
    List<List<String>> wholeTable = new ArrayList<>(  );

    private Explore() {

    }


    public synchronized static Explore getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new Explore();
        }
        return INSTANCE;
    }


    public String testing() throws Exception {
        getStatistics();
        return prepareUserInput();
    }


    public String prepareUserInput() throws Exception {
        /*
        String userInput = classifiedData.toString().replace( " ", ";" );
        int countLines = userInput.length() - userInput.replace( "=", "" ).length();

        String[] userClassification = userInput.split( ";" );

        System.out.println( Arrays.toString( userClassification ) );
        System.out.println( userClassification.length );

        List<String[]> answers = new ArrayList<>();

        int dim = 0;
        for ( int i = 0; i < userClassification.length; i++ ) {
            String[] splited = userClassification[i].replace( "{", "" ).replace( "}", "" ).split( "[,=]" );

            System.out.println( Arrays.toString( splited ) );
            dim = splited.length;
            System.out.println( dim );
            answers.add( splited );

        }
        */


        String[][] rotated = rotate2dArray( classifiedData );

        return convertToArff( rotated, classifiedData[0].length, classifiedData );
    }


    public String convertToArff( String[][] rotated, int dimLength, String[][] answers ) throws Exception {

        int numInstances = rotated[0].length;
        FastVector atts = new FastVector();
        FastVector attVals;
        FastVector attValsEl[] = new FastVector[dimLength];
        Instances data;
        double[] vals;
        //String[] values = new String[numInstances];
        //Set<String> doublicates = new HashSet<>();

        // attributes
        for ( int dim = 0; dim < dimLength; dim++ ) {
            attVals = new FastVector();

            for(int i = 0; i < allUniqueValues.get( dim ).size(); i++){
                attVals.addElement( allUniqueValues.get( dim ).get( i ) );
            }


            /*
            for ( int obj = 0; obj < numInstances; obj++ ) {
                values[obj] = rotated[dim][obj];
            }
            //System.out.println( Arrays.toString( values ) );

            for ( String val : values ) {
                if ( doublicates.add( val ) ) {
                    attVals.addElement( val );
                }
            }
             */

            atts.addElement( new Attribute( "attr" + dim, attVals ) );
            attValsEl[dim] = attVals;
        }
        // instances object
        data = new Instances( "Dataset", atts, 0 );

        // fill data
        for ( int obj = 0; obj < numInstances; obj++ ) {
            vals = new double[data.numAttributes()];
            for ( int dim = 0; dim < dimLength; dim++ ) {
                vals[dim] = attValsEl[dim].indexOf( answers[obj][dim] );
            }
            data.add( new DenseInstance( 1.0, vals ) );
        }

        System.out.println( data );

        //saveAsArff( data, "test.arff" );
        return trainData(data);
        //trainData();
    }


    public String trainData(Instances data) throws Exception {
        //Instances data = getDataSet( "explore-by-example/test.arff" );

        data.setClassIndex( data.numAttributes() -1 );
        J48 tree = new J48();

        String[] options = {"-U"};
        tree.setOptions( options );

        tree.buildClassifier( data );


        Instances unlabeled = new Instances(  new BufferedReader( new FileReader("explore-by-example/exploreExample.arff" ) ));

        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );

        Instances labeled = new Instances( unlabeled );

        String labledData;
        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            labeled.instance( i ).setClassValue( clsLabel );
            System.out.println( clsLabel + " -> " + unlabeled.classAttribute().value( (int) clsLabel ) );
        }


        saveAsArff( labeled, "labeled-data.arff" );
        labledData = String.valueOf( labeled );
        return labledData;
    }


    private void getStatistics() {
        StatisticsManager<?> stats = StatisticsManager.getInstance();
        List<StatisticQueryColumn> uniqueValues = stats.getAllUniqueValues( Arrays.asList( columnId ), tableId);

        for ( StatisticQueryColumn uniqueValue : uniqueValues ) {
            allUniqueValues.add( Arrays.asList( uniqueValue.getData() ) );
        }

        List<String> trueFalse = new ArrayList<>(  );
        trueFalse.add( "true" );
        trueFalse.add("false");
        allUniqueValues.add(trueFalse);
        System.out.println( allUniqueValues );


        StatisticResult statisticResult = stats.getTable(columnId , tableId);

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
