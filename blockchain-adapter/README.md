# Blockchain Adapter - Ethereum

It uses the [Ethereum JSON RPC API](https://ethereum.org/en/developers/docs/apis/json-rpc/) to query the blockchain.



## Setup

1. Follow installation for [GEth](https://geth.ethereum.org/)
2. Start the [GEth Client with RPC](https://geth.ethereum.org/docs/rpc/server)

You can now use the the adapter to query this endpoint

## Parameters

 - `ClientUrl` - The JSON RPC Endpoint
 - `Blocks` - The adapter maintains a Running window over the blockchain for queries, this parameter specifies how many blocks to include in the running window. 
       _e.g Blocks = 10 means every query will run over the 10 latest blocks_
 - `ExperimentalFiltering` - It will try to fetch historic blocks beyond the Running Window, by guessing the block numbers that can be fetched by the current query only works on `BlockNumber` fields i.e `number` field in _blocks table_  and `blockNumber` filed in _transactions table_. This is extremely experimental and might not give expected results in some cases.
 
      
         
 

