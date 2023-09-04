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

package org.polypheny.db.mqtt;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.stream.StreamProcessor;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Slf4j
public class MqttStreamProcessor implements StreamProcessor {

    private final MqttMessage mqttMessage;
    private final String filterQuery;
    private final StreamProcessor streamProcessor;

    private Statement statement;


    public MqttStreamProcessor( MqttMessage mqttMessage,String filterQuery, Statement statement ) {
        this.mqttMessage = mqttMessage;
        this.filterQuery = filterQuery;
        this.streamProcessor = statement.getStreamProcessor( mqttMessage.getMessage() );
        this.statement = statement;
    }


    public boolean processStream() {
        AlgRoot root = processMqlQuery(); //buildFilter(); TODO
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
        log.info( res.toString() );
        return !res.isEmpty();

    }


    private AlgRoot processMqlQuery() {
        AlgBuilder algBuilder = AlgBuilder.create( this.statement );

        Processor mqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "mongo" ) );
        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        final AlgOptCluster cluster = AlgOptCluster.createDocument( statement.getQueryProcessor().getPlanner(), algBuilder.getRexBuilder() );

        MqlToAlgConverter mqlConverter = new MqlToAlgConverter( mqlProcessor, catalogReader, cluster );

        // QueryParameters parameters = new MqlQueryParameters( this.filterQuery, Catalog.getInstance().getDatabase( Catalog.defaultDatabaseId ).name,NamespaceType.DOCUMENT );

        MqlFind find = (MqlFind) mqlProcessor.parse( String.format( "db.%s.find(%s)", "collection", this.filterQuery ) ).get( 0 );

        final AlgDataType rowType =
                cluster.getTypeFactory()
                        .builder()
                        .add( "value", null, PolyType.INTEGER )
                        .nullable( false )
                        .build();
        final ImmutableList<ImmutableList<RexLiteral>> tuples =
                ImmutableList.of(
                        ImmutableList.of( (RexLiteral)
                                cluster.getRexBuilder().makeLiteral(
                                        Integer.parseInt( this.mqttMessage.getMessage() ),
                                        rowType.getFieldList().get( 0 ).getType(),
                                        true) ) );

/*
        RexCall queryValue = new RexCall(
                cluster.getTypeFactory().createPolyType( PolyType.ANY ),
                OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_QUERY_VALUE ),
                Arrays.asList(
                        RexInputRef.of( 0, rowType ),
                        filter ) );
        */

        AlgTraitSet docTraitSet = cluster.traitSet().replace( ModelTrait.DOCUMENT );
        LogicalValues msgValue = new LogicalValues( cluster, docTraitSet, rowType, tuples );
        //AlgNode input = LogicalDocumentValues.create( msgValue );


        BsonDocument msgDoc = new BsonDocument("value", new BsonInt32( Integer.parseInt( this.mqttMessage.getMessage() ) ) );
        List<AlgDataTypeField> fields = new ArrayList<>();
        fields.add( new AlgDataTypeFieldImpl( "d", 0, algBuilder.getTypeFactory().createPolyType( PolyType.DOCUMENT ) ) );
        AlgDataType defaultRowType = new AlgRecordType( fields );

        AlgNode input = new LogicalDocumentValues( cluster, defaultRowType, docTraitSet,ImmutableList.of( msgDoc ) );
        //AlgNode input = LogicalDocumentValues.create( cluster, ImmutableList.of( msgDoc ) );
        AlgRoot root;
        root = mqlConverter.convert( find, input );



        // insert message in alg tree
        if ( this.mqttMessage.getMessage().contains( "[" ) ) {
            //msg is array -> save with MQL_ITEM
            //TODO: implekmt
            /*
            String[] msgArray = this.mqttMessage.getMessage().replace( "[", "" ).replace( "]", "" ).split( "," );
            BsonArray bsonValues = new BsonArray(msgArray.length);
            for ( int i = 0; i < msgArray.length; i++ ) {
                bsonValues.add( new BsonString( msgArray[i].trim() ) );
            }
            List<RexNode> nodes = convertArray( key, bsonValues, rowType );
            RexNode call = new RexCall( any, OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_ITEM ), Arrays.asList( nodes.get( 0 ), nodes.get( 1 ) ) );
*/

// l. 2198 MqltoALg
            /*
            private RexCall getStringArray( List<String> elements ) {
                List<RexNode> rexNodes = new ArrayList<>();
                int maxSize = 0;
                for ( String name : elements ) {
                    rexNodes.add( convertLiteral( new BsonString( name ) ) );
                    maxSize = Math.max( name.length(), maxSize );
                }

                AlgDataType type = cluster.getTypeFactory().createArrayType(
                        cluster.getTypeFactory().createPolyType( PolyType.CHAR, maxSize ),
                        rexNodes.size() );
                return getArray( rexNodes, type );

*/
            root = null;
            //---------------------------------------------------------------------


        } else if ( this.mqttMessage.getMessage().contains( "{" ) ) {
            // msg in JSON format
            //TODO: implekmt
            /*
            RexLiteral key = rexBuilder.makeLiteral( find.get );
            if ( values.get( "" ).isDocument() ) {
                comparisionValue = values.get( "" ).asDocument();
            }
            call = rexBuilder.makeCall( OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_EXISTS,  );

             */
            root = null;
        // ---------------------------------------------------------------------
        
            

        } else {
            //TODO: if msg is only one value: then field/key is empty String
            // Abfrage ob value zum key Document ist weil dann gibt es einen anderen Operator als equals.
            // only one value:

            // build Document out of message value:
            // TODO: at end: change type of message to Object, so that also ints are supported



            if ( find.getQuery().get( "value" ).isDocument() ) {
                String parentkey = "";
                // TODO: key is then: parentkey.key
                /*
                // other op than eq:
                BsonDocument doc = values.get( "" ).asDocument();
                if ( doc.getFirstKey().equals( "" ) ) {

                } else if ( doc.getFirstKey().equals( "" ) ) {

                }
                AlgNode node = LogicalDocumentValues.create( algBuilder.getCluster(), ImmutableList.of( msgDoc ) );
                */

                root = null;
            } else {
                // op is equals
                /*
                RexNode queryLiteral = algBuilder.literal( find.getQuery().get( "value" ).asInt32().getValue() );
                RexInputRef msgRef = cluster.getRexBuilder().makeInputRef( input,0 );
                // build condition for where clause (=condition for saving)
                AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
                String[] fieldName ={"value"};
                AlgNode filter = algBuilder
                        //.values( fieldName, Integer.parseInt( this.mqttMessage.getMessage() ) )
                        //algBuilder.call( OperatorRegistry.get( OperatorName.EQUALS ), algBuilder.field( "value" ), queryLiteral )
                        .filter( new RexCall( type, OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_EQUALS ), Arrays.asList( msgRef, queryLiteral ) ) )
                        .build();
                root = AlgRoot.of( filter, Kind.FILTER );
*/
            }
        }


        // MqlToAlgConverter 1467
        /*
        if ( key.equals( "$exists" ) ) {
            return convertExists( bsonValue, key, rowType );

        }


        // TODO: check if attibute exists
                
         */
        return root;
    }


    private RexNode translateQuery(BsonDocument bsonDocument, AlgDataType rowType, String parentKey, AlgBuilder algBuilder) {
        // TODO: for-loop for more than one predicates
        // parentkey = null
        List<RexNode> nodes = new ArrayList<>();
        nodes.add( attachRef( bsonDocument.getFirstKey(), rowType ) );

        if ( bsonDocument.get( bsonDocument.getFirstKey() ).isArray() ) {
            /* TODO
            List<RexNode> arr = convertArray( bsonDocument.getFirstKey(), bsonDocument.get( bsonDocument.getFirstKey() ).asArray(), true, rowType, "" );
            nodes.add( getArray(arr, algBuilder.getCluster().getTypeFactory().createArrayType( nullableAny, arr.size() ) ) );
             */
            //List<RexNode> arr = bsonValue.asArray().stream().map( this::convertLiteral ).collect( Collectors.toList() );
            //queryLiteral = getArray( arr, any );

        } else {
            AlgDataType type = algBuilder.getCluster().getTypeFactory().createPolyType( getPolyType( bsonDocument.get( bsonDocument.getFirstKey() ) ) );
            RexLiteral queryLiteral;
            Pair<Comparable, PolyType> valuePair = RexLiteral.convertType( getComparable(  bsonDocument.get( bsonDocument.getFirstKey() ) ), type );
            queryLiteral = new RexLiteral( valuePair.left, type, valuePair.right );
            nodes.add( queryLiteral );
        }
        AlgDataType type = algBuilder.getTypeFactory().createTypeWithNullability( algBuilder.getTypeFactory().createPolyType( PolyType.BOOLEAN ), true );
        RexNode predicate = new RexCall( type, OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_EQUALS ), nodes );
        return predicate;

            //return getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );
    }


    private Comparable getComparable( BsonValue value ) {
        switch ( value.getBsonType() ) {
            case INT32:
                return value.asInt32().getValue();
            case DOUBLE:
                return value.asDouble().getValue();
            case STRING:
                return value.asString().getValue();
        }
        throw new RuntimeException( "Not implemented Comparable transform" );
    }


    private PolyType getPolyType( BsonValue bsonValue ) {
        switch ( bsonValue.getBsonType() ) {
            case DOUBLE:
                return PolyType.DOUBLE;
            case STRING:
                return PolyType.CHAR;
            case DOCUMENT:
                return PolyType.JSON;
            case ARRAY:
                break;
            case BOOLEAN:
                return PolyType.BOOLEAN;
            case INT32:
                return PolyType.INTEGER;
        }
        throw new RuntimeException( "PolyType not implemented " );
    }

/*
    private AlgNode buildDocumentFilter( BsonDocument filterDoc, AlgNode msgNode, AlgDataType rowType ) {

        ArrayList<RexNode> operands = new ArrayList<>();

        for ( Entry<String, BsonValue> entry : filterDoc.entrySet() ) {
            if ( entry.getKey().equals( "$regex" ) ) {
                operands.add( convertRegex( filterDoc, parentKey, rowType ) );
            } else if ( !entry.getKey().equals( "$options" ) ) {
                // normal handling
                operands.add( convertEntry( entry.getKey(), parentKey, entry.getValue(), rowType ) );
            }
        }
        RexNode condition = getFixedCall( operands, OperatorRegistry.get( OperatorName.AND ), PolyType.BOOLEAN );

        return LogicalDocumentFilter.create( msgNode, condition );
    }

 */


    private RexNode attachRef( String key, AlgDataType rowType ) {
        AlgDataTypeField field = rowType.getField( key, false, false );
        return RexInputRef.of( field.getIndex(), rowType );
    }


    @Override
    public String getStream() {
        return streamProcessor.getStream();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validateContent( String msg ) {
        //TODO: Implement
        return true;
    }


    private String extractValue( String attributeName ) {
        attributeName = attributeName;
        int attributeStartIndex = this.mqttMessage.getMessage().indexOf( attributeName );
        if ( attributeStartIndex == -1 ) {
            throw new RuntimeException( "The specified attribute could not be found in the received message!" );
        }
        int attributeEndIndex = attributeStartIndex + attributeName.length();
        int valueStartIndex = this.mqttMessage.getMessage().indexOf( ":", attributeEndIndex );
        int valueEndIndex = this.mqttMessage.getMessage().indexOf( ",", attributeEndIndex );
        //TODO: Problem, wenn attribute als letztes ist, weil danach kein komma kommt sondern }.
        //TODO: kann probleme geben, wenn comparission value mit " ist und attributeValue ohne "
        String attributeValue = this.mqttMessage.getMessage().substring( valueStartIndex, valueEndIndex ).trim().replaceAll( "\"", "" );
        return attributeValue;
    }


    public AlgRoot buildFilter() {
        AlgBuilder algBuilder = AlgBuilder.create( this.statement );
        //TODO: change if this is doch needed:
        List<RexNode> predicatesList  = buildPredicateRexNodes(this.filterQuery, algBuilder);
        AlgNode algNode = algBuilder.filter( predicatesList ).build();
        return AlgRoot.of( algNode, Kind.FILTER );


/*
        String[]  fieldname = {this.receivedMqttMessage.getTopic()};
        // --------- simple query -----------
        // extract operator for query
        Operator queryOperator;
        if ( query != null ) {
            String[] queryArray = query.split( " " );
            switch ( queryArray[0].trim() ) {
                case "<":
                    queryOperator = OperatorRegistry.get( OperatorName.LESS_THAN );
                    break;
                case "<=":
                    queryOperator = OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
                    break;
                case ">":
                    queryOperator = OperatorRegistry.get( OperatorName.GREATER_THAN );
                    break;
                case ">=":
                    queryOperator = OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
                    break;
                case "=":
                    queryOperator = OperatorRegistry.get( OperatorName.EQUALS );
                    break;
                case "<>":
                    queryOperator = OperatorRegistry.get( OperatorName.NOT_EQUALS );
                    break;
                default:
                    throw new RuntimeException( "The operator in the filter query could not be identified!" );
            }
            // extract other literal from query
            RexNode queryLiteral = algBuilder.literal( queryArray[1].trim() );

            // build condition for where clause (=condition for saving)
             =algBuilder
                    .values( fieldname, this.receivedMqttMessage.getMessage() )
                    .filter( algBuilder.call( queryOperator, algBuilder.field( this.receivedMqttMessage.getTopic() ), queryLiteral ) )
                    .build();

        }

 */
    }


    private List<RexNode> buildPredicateRexNodes(String predicatesArray, AlgBuilder algBuilder) {

        List<RexNode> predicateList = new ArrayList<>();
        /*
            // means query has a simple form: <operator> <literal>

            // complex form means query has attribute names in it: <attributeName> <operator> <literal>
            for ( int i = 0; i < predicatesArray.length; i++ ) {
                int operatorIndex;
                int lengthOfOperator;
                Operator predicateOperator;
                if ( predicatesArray[i].contains( "<" ) ) {
                    operatorIndex = predicatesArray[i].indexOf( "<" );
                    lengthOfOperator = 1;
                    predicateOperator = OperatorRegistry.get( OperatorName.LESS_THAN );
                } else if ( predicatesArray[i].contains( "<=" ) ) {
                    operatorIndex = predicatesArray[i].indexOf( "<=" );
                    lengthOfOperator = 2;
                    predicateOperator = OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
                } else if ( predicatesArray[i].contains( ">" ) ) {
                    operatorIndex = predicatesArray[i].indexOf( ">" );
                    lengthOfOperator = 1;
                    predicateOperator = OperatorRegistry.get( OperatorName.GREATER_THAN );
                } else if ( predicatesArray[i].contains( ">=" ) ) {
                    operatorIndex = predicatesArray[i].indexOf( ">=" );
                    lengthOfOperator = 2;
                    predicateOperator = OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
                } else if ( predicatesArray[i].contains( "=" ) ) {
                    operatorIndex = predicatesArray[i].indexOf( "=" );
                    lengthOfOperator = 1;
                    predicateOperator = OperatorRegistry.get( OperatorName.EQUALS );
                } else if ( predicatesArray[i].contains( "<>" ) ) {
                    operatorIndex = predicatesArray[i].indexOf( "<>" );
                    lengthOfOperator = 2;
                    predicateOperator = OperatorRegistry.get( OperatorName.NOT_EQUALS );
                } else {
                    throw new RuntimeException( "MqttStreamProcessor could not recognize an operator." );
                }
                RexNode comparisionLiteral = algBuilder.literal( predicatesArray[i].substring( operatorIndex + lengthOfOperator ).trim() );
                String[] attributeName = new String[1];
                String attributeValue;
                if ( predicatesArray.length == 1 ) {
                    attributeName[0] = this.mqttMessage.getTopic();
                    attributeValue = this.mqttMessage.getMessage();
                } else {
                    attributeName[0] = predicatesArray[i].substring( 0, operatorIndex ).trim();
                    attributeValue = extractValue( attributeName[0] );
                    if ( !validateContent( attributeValue ) ) {
                        throw new RuntimeException( "Content value was not valid." );
                    }
                }
                algBuilder.values( attributeName, attributeValue );
                RexNode predicateNode = algBuilder.call(predicateOperator, algBuilder.field( attributeName[0] ), comparisionLiteral );
                predicateList.add( predicateNode );
            }

         */
        return predicateList;
    }


    List<List<Object>> executeAndTransformPolyAlg( AlgRoot algRoot, Statement statement, final Context ctx ) {

        try {
            // Prepare
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, false );
            log.debug( "AlgRoot was prepared." );

            List<List<Object>> rows = result.getRows( statement, -1 );
            statement.getTransaction().commit();
            return rows;
        } catch ( Throwable e ) {
            log.error( "Error during execution of stream processor query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
    }


    /**
     * saves predicatesString in array seperated by commas
     * @param predicatesString
     * @return
     */
    private String[] saveInArray( String predicatesString ) {
        if ( predicatesString != null ) {
            String[] queryArray = predicatesString.split( "," );
            for ( int i = 0; i < queryArray.length; i++ ) {
                queryArray[i] = queryArray[i].trim();
            }
            return queryArray;
        } else {
            return new String[0];
        }
    }

}
