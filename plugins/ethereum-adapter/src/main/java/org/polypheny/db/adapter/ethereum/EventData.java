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

package org.polypheny.db.adapter.ethereum;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Bool;
import org.web3j.abi.datatypes.DynamicBytes;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Utf8String;
import org.web3j.abi.datatypes.generated.Uint8;
import org.web3j.abi.datatypes.generated.Uint16;
import org.web3j.abi.datatypes.generated.Uint24;
import org.web3j.abi.datatypes.generated.Uint32;
import org.web3j.abi.datatypes.generated.Uint40;
import org.web3j.abi.datatypes.generated.Uint48;
import org.web3j.abi.datatypes.generated.Uint56;
import org.web3j.abi.datatypes.generated.Uint64;
import org.web3j.abi.datatypes.generated.Uint72;
import org.web3j.abi.datatypes.generated.Uint80;
import org.web3j.abi.datatypes.generated.Uint88;
import org.web3j.abi.datatypes.generated.Uint96;
import org.web3j.abi.datatypes.generated.Uint104;
import org.web3j.abi.datatypes.generated.Uint112;
import org.web3j.abi.datatypes.generated.Uint120;
import org.web3j.abi.datatypes.generated.Uint128;
import org.web3j.abi.datatypes.generated.Uint136;
import org.web3j.abi.datatypes.generated.Uint144;
import org.web3j.abi.datatypes.generated.Uint152;
import org.web3j.abi.datatypes.generated.Uint160;
import org.web3j.abi.datatypes.generated.Uint168;
import org.web3j.abi.datatypes.generated.Uint176;
import org.web3j.abi.datatypes.generated.Uint184;
import org.web3j.abi.datatypes.generated.Uint192;
import org.web3j.abi.datatypes.generated.Uint200;
import org.web3j.abi.datatypes.generated.Uint208;
import org.web3j.abi.datatypes.generated.Uint216;
import org.web3j.abi.datatypes.generated.Uint224;
import org.web3j.abi.datatypes.generated.Uint232;
import org.web3j.abi.datatypes.generated.Uint240;
import org.web3j.abi.datatypes.generated.Uint248;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.abi.datatypes.generated.Int8;
import org.web3j.abi.datatypes.generated.Int16;
import org.web3j.abi.datatypes.generated.Int24;
import org.web3j.abi.datatypes.generated.Int32;
import org.web3j.abi.datatypes.generated.Int40;
import org.web3j.abi.datatypes.generated.Int48;
import org.web3j.abi.datatypes.generated.Int56;
import org.web3j.abi.datatypes.generated.Int64;
import org.web3j.abi.datatypes.generated.Int72;
import org.web3j.abi.datatypes.generated.Int80;
import org.web3j.abi.datatypes.generated.Int88;
import org.web3j.abi.datatypes.generated.Int96;
import org.web3j.abi.datatypes.generated.Int104;
import org.web3j.abi.datatypes.generated.Int112;
import org.web3j.abi.datatypes.generated.Int120;
import org.web3j.abi.datatypes.generated.Int128;
import org.web3j.abi.datatypes.generated.Int136;
import org.web3j.abi.datatypes.generated.Int144;
import org.web3j.abi.datatypes.generated.Int152;
import org.web3j.abi.datatypes.generated.Int160;
import org.web3j.abi.datatypes.generated.Int168;
import org.web3j.abi.datatypes.generated.Int176;
import org.web3j.abi.datatypes.generated.Int184;
import org.web3j.abi.datatypes.generated.Int192;
import org.web3j.abi.datatypes.generated.Int200;
import org.web3j.abi.datatypes.generated.Int208;
import org.web3j.abi.datatypes.generated.Int216;
import org.web3j.abi.datatypes.generated.Int224;
import org.web3j.abi.datatypes.generated.Int232;
import org.web3j.abi.datatypes.generated.Int240;
import org.web3j.abi.datatypes.generated.Int248;
import org.web3j.abi.datatypes.generated.Int256;
import org.web3j.abi.datatypes.generated.Bytes1;
import org.web3j.abi.datatypes.generated.Bytes2;
import org.web3j.abi.datatypes.generated.Bytes3;
import org.web3j.abi.datatypes.generated.Bytes4;
import org.web3j.abi.datatypes.generated.Bytes5;
import org.web3j.abi.datatypes.generated.Bytes6;
import org.web3j.abi.datatypes.generated.Bytes7;
import org.web3j.abi.datatypes.generated.Bytes8;
import org.web3j.abi.datatypes.generated.Bytes9;
import org.web3j.abi.datatypes.generated.Bytes10;
import org.web3j.abi.datatypes.generated.Bytes11;
import org.web3j.abi.datatypes.generated.Bytes12;
import org.web3j.abi.datatypes.generated.Bytes13;
import org.web3j.abi.datatypes.generated.Bytes14;
import org.web3j.abi.datatypes.generated.Bytes15;
import org.web3j.abi.datatypes.generated.Bytes16;
import org.web3j.abi.datatypes.generated.Bytes17;
import org.web3j.abi.datatypes.generated.Bytes18;
import org.web3j.abi.datatypes.generated.Bytes19;
import org.web3j.abi.datatypes.generated.Bytes20;
import org.web3j.abi.datatypes.generated.Bytes21;
import org.web3j.abi.datatypes.generated.Bytes22;
import org.web3j.abi.datatypes.generated.Bytes23;
import org.web3j.abi.datatypes.generated.Bytes24;
import org.web3j.abi.datatypes.generated.Bytes25;
import org.web3j.abi.datatypes.generated.Bytes26;
import org.web3j.abi.datatypes.generated.Bytes27;
import org.web3j.abi.datatypes.generated.Bytes28;
import org.web3j.abi.datatypes.generated.Bytes29;
import org.web3j.abi.datatypes.generated.Bytes30;
import org.web3j.abi.datatypes.generated.Bytes31;
import org.web3j.abi.datatypes.generated.Bytes32;


public class EventData {

    @Getter
    private String originalKey;
    @Getter
    private Event event;
    @Getter
    private String smartContractAddress;
    @Getter
    private String compositeName;
    @Getter
    private JSONArray abiInputs;


    public EventData( String originalKey, String contractName, String smartContractAddress, JSONArray abiInputs ) {
        this.originalKey = originalKey;
        this.compositeName = contractName.toLowerCase() + "_" + originalKey.toLowerCase();
        this.abiInputs = abiInputs;
        List<TypeReference<?>> typeReferences = createTypeReferences( abiInputs );
        this.event = new Event( originalKey, typeReferences ); // create event based on event name (original key and inputs)
        this.smartContractAddress = smartContractAddress;
    }


    private static List<TypeReference<?>> createTypeReferences( JSONArray abiInputs ) {
        List<TypeReference<?>> typeReferences = new ArrayList<>();
        for ( int i = 0; i < abiInputs.length(); i++ ) {
            JSONObject inputObject = abiInputs.getJSONObject( i );
            String type = inputObject.getString( "type" );
            boolean indexed = inputObject.getBoolean( "indexed" );

            switch ( type ) {
                case "address":
                    typeReferences.add( new TypeReference<Address>( indexed ) {
                    } );
                    break;
                case "bool":
                    typeReferences.add( new TypeReference<Bool>( indexed ) {
                    } );
                    break;
                case "string":
                    typeReferences.add( new TypeReference<Utf8String>( indexed ) {
                    } );
                    break;
                case "unit":
                    typeReferences.add( new TypeReference<Uint256>( indexed ) {
                    } );
                    break;
                case "int":
                    typeReferences.add( new TypeReference<Int256>( indexed ) {
                    } );
                    break;
                case "bytes": // dynamic-sized byte array
                    typeReferences.add( new TypeReference<DynamicBytes>( indexed ) {
                    } );
                    break;
                default:
                    if ( type.startsWith( "uint" ) ) {
                        int bitSize = Integer.parseInt( type.substring( 4 ) ); // Get the bit size, e.g., 8 from uint8
                        typeReferences.add( createUintTypeReference( bitSize, indexed ) );
                    } else if ( type.startsWith( "int" ) ) {
                        int bitSize = Integer.parseInt( type.substring( 4 ) ); // Get the bit size, e.g., 8 from int8
                        typeReferences.add( createIntTypeReference( bitSize, indexed ) );
                    } else if ( type.startsWith( "bytes" ) && !type.equals( "bytes" ) ) { // fixed-sized byte array
                        int size = Integer.parseInt( type.substring( 5 ) ); // Get size, e.g., 1 from bytes1
                        typeReferences.add( createBytesTypeReference( size, indexed ) );
                    }
                    break;
            }
        }

        return typeReferences;

    }


    private static TypeReference<?> createUintTypeReference( int bitSize, boolean indexed ) {
        switch ( bitSize ) {
            case 8:
                return new TypeReference<Uint8>( indexed ) {
                };
            case 16:
                return new TypeReference<Uint16>( indexed ) {
                };
            case 24:
                return new TypeReference<Uint24>( indexed ) {
                };
            case 32:
                return new TypeReference<Uint32>( indexed ) {
                };
            case 40:
                return new TypeReference<Uint40>( indexed ) {
                };
            case 48:
                return new TypeReference<Uint48>( indexed ) {
                };
            case 56:
                return new TypeReference<Uint56>( indexed ) {
                };
            case 64:
                return new TypeReference<Uint64>( indexed ) {
                };
            case 72:
                return new TypeReference<Uint72>( indexed ) {
                };
            case 80:
                return new TypeReference<Uint80>( indexed ) {
                };
            case 88:
                return new TypeReference<Uint88>( indexed ) {
                };
            case 96:
                return new TypeReference<Uint96>( indexed ) {
                };
            case 104:
                return new TypeReference<Uint104>( indexed ) {
                };
            case 112:
                return new TypeReference<Uint112>( indexed ) {
                };
            case 120:
                return new TypeReference<Uint120>( indexed ) {
                };
            case 128:
                return new TypeReference<Uint128>( indexed ) {
                };
            case 136:
                return new TypeReference<Uint136>( indexed ) {
                };
            case 144:
                return new TypeReference<Uint144>( indexed ) {
                };
            case 152:
                return new TypeReference<Uint152>( indexed ) {
                };
            case 160:
                return new TypeReference<Uint160>( indexed ) {
                };
            case 168:
                return new TypeReference<Uint168>( indexed ) {
                };
            case 176:
                return new TypeReference<Uint176>( indexed ) {
                };
            case 184:
                return new TypeReference<Uint184>( indexed ) {
                };
            case 192:
                return new TypeReference<Uint192>( indexed ) {
                };
            case 200:
                return new TypeReference<Uint200>( indexed ) {
                };
            case 208:
                return new TypeReference<Uint208>( indexed ) {
                };
            case 216:
                return new TypeReference<Uint216>( indexed ) {
                };
            case 224:
                return new TypeReference<Uint224>( indexed ) {
                };
            case 232:
                return new TypeReference<Uint232>( indexed ) {
                };
            case 240:
                return new TypeReference<Uint240>( indexed ) {
                };
            case 248:
                return new TypeReference<Uint248>( indexed ) {
                };
            case 256:
                return new TypeReference<Uint256>( indexed ) {
                };
            default:
                throw new IllegalArgumentException( "Unsupported bit size: " + bitSize );
        }
    }


    private static TypeReference<?> createIntTypeReference( int bitSize, boolean indexed ) {
        switch ( bitSize ) {
            case 8:
                return new TypeReference<Int8>( indexed ) {
                };
            case 16:
                return new TypeReference<Int16>( indexed ) {
                };
            case 24:
                return new TypeReference<Int24>( indexed ) {
                };
            case 32:
                return new TypeReference<Int32>( indexed ) {
                };
            case 40:
                return new TypeReference<Int40>( indexed ) {
                };
            case 48:
                return new TypeReference<Int48>( indexed ) {
                };
            case 56:
                return new TypeReference<Int56>( indexed ) {
                };
            case 64:
                return new TypeReference<Int64>( indexed ) {
                };
            case 72:
                return new TypeReference<Int72>( indexed ) {
                };
            case 80:
                return new TypeReference<Int80>( indexed ) {
                };
            case 88:
                return new TypeReference<Int88>( indexed ) {
                };
            case 96:
                return new TypeReference<Int96>( indexed ) {
                };
            case 104:
                return new TypeReference<Int104>( indexed ) {
                };
            case 112:
                return new TypeReference<Int112>( indexed ) {
                };
            case 120:
                return new TypeReference<Int120>( indexed ) {
                };
            case 128:
                return new TypeReference<Int128>( indexed ) {
                };
            case 136:
                return new TypeReference<Int136>( indexed ) {
                };
            case 144:
                return new TypeReference<Int144>( indexed ) {
                };
            case 152:
                return new TypeReference<Int152>( indexed ) {
                };
            case 160:
                return new TypeReference<Int160>( indexed ) {
                };
            case 168:
                return new TypeReference<Int168>( indexed ) {
                };
            case 176:
                return new TypeReference<Int176>( indexed ) {
                };
            case 184:
                return new TypeReference<Int184>( indexed ) {
                };
            case 192:
                return new TypeReference<Int192>( indexed ) {
                };
            case 200:
                return new TypeReference<Int200>( indexed ) {
                };
            case 208:
                return new TypeReference<Int208>( indexed ) {
                };
            case 216:
                return new TypeReference<Int216>( indexed ) {
                };
            case 224:
                return new TypeReference<Int224>( indexed ) {
                };
            case 232:
                return new TypeReference<Int232>( indexed ) {
                };
            case 240:
                return new TypeReference<Int240>( indexed ) {
                };
            case 248:
                return new TypeReference<Int248>( indexed ) {
                };
            case 256:
                return new TypeReference<Int256>( indexed ) {
                };
            default:
                throw new IllegalArgumentException( "Unsupported bit size: " + bitSize );
        }
    }


    private static TypeReference<?> createBytesTypeReference( int size, boolean indexed ) {
        switch ( size ) {
            case 1:
                return new TypeReference<Bytes1>( indexed ) {
                };
            case 2:
                return new TypeReference<Bytes2>( indexed ) {
                };
            case 3:
                return new TypeReference<Bytes3>( indexed ) {
                };
            case 4:
                return new TypeReference<Bytes4>( indexed ) {
                };
            case 5:
                return new TypeReference<Bytes5>( indexed ) {
                };
            case 6:
                return new TypeReference<Bytes6>( indexed ) {
                };
            case 7:
                return new TypeReference<Bytes7>( indexed ) {
                };
            case 8:
                return new TypeReference<Bytes8>( indexed ) {
                };
            case 9:
                return new TypeReference<Bytes9>( indexed ) {
                };
            case 10:
                return new TypeReference<Bytes10>( indexed ) {
                };
            case 11:
                return new TypeReference<Bytes11>( indexed ) {
                };
            case 12:
                return new TypeReference<Bytes12>( indexed ) {
                };
            case 13:
                return new TypeReference<Bytes13>( indexed ) {
                };
            case 14:
                return new TypeReference<Bytes14>( indexed ) {
                };
            case 15:
                return new TypeReference<Bytes15>( indexed ) {
                };
            case 16:
                return new TypeReference<Bytes16>( indexed ) {
                };
            case 17:
                return new TypeReference<Bytes17>( indexed ) {
                };
            case 18:
                return new TypeReference<Bytes18>( indexed ) {
                };
            case 19:
                return new TypeReference<Bytes19>( indexed ) {
                };
            case 20:
                return new TypeReference<Bytes20>( indexed ) {
                };
            case 21:
                return new TypeReference<Bytes21>( indexed ) {
                };
            case 22:
                return new TypeReference<Bytes22>( indexed ) {
                };
            case 23:
                return new TypeReference<Bytes23>( indexed ) {
                };
            case 24:
                return new TypeReference<Bytes24>( indexed ) {
                };
            case 25:
                return new TypeReference<Bytes25>( indexed ) {
                };
            case 26:
                return new TypeReference<Bytes26>( indexed ) {
                };
            case 27:
                return new TypeReference<Bytes27>( indexed ) {
                };
            case 28:
                return new TypeReference<Bytes28>( indexed ) {
                };
            case 29:
                return new TypeReference<Bytes29>( indexed ) {
                };
            case 30:
                return new TypeReference<Bytes30>( indexed ) {
                };
            case 31:
                return new TypeReference<Bytes31>( indexed ) {
                };
            case 32:
                return new TypeReference<Bytes32>( indexed ) {
                };
            default:
                throw new IllegalArgumentException( "Size not supported for Bytes type." );
        }

    }

}



