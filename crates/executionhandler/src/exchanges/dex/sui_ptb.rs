//! SUI Programmable Transaction Block (PTB) Builder
//!
//! Provides structures and utilities for building SUI transactions using
//! the Programmable Transaction Block format introduced in SUI Move.

use serde::{Deserialize, Serialize};
use crate::core::types::ExecutionError;

/// Object reference (object ID + version + digest)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRef {
    pub object_id: String,
    pub version: u64,
    pub digest: String,
}

/// Argument to a PTB command
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum Argument {
    /// Gas coin
    GasCoin,
    /// Input argument (from transaction inputs)
    Input { index: u16 },
    /// Result from a previous command
    Result { cmd_index: u16 },
    /// Nested result (result from previous command, specific element)
    NestedResult { cmd_index: u16, result_index: u16 },
}

/// Type tag for Move types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeTag {
    pub type_: String,
}

impl TypeTag {
    pub fn new(type_str: &str) -> Self {
        Self {
            type_: type_str.to_string(),
        }
    }
}

/// PTB Command
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "PascalCase")]
pub enum Command {
    /// Split coins into specified amounts
    SplitCoins {
        coin: Argument,
        amounts: Vec<Argument>,
    },
    
    /// Merge coins together
    MergeCoins {
        destination: Argument,
        sources: Vec<Argument>,
    },
    
    /// Call a Move function
    MoveCall {
        package: String,
        module: String,
        function: String,
        type_arguments: Vec<TypeTag>,
        arguments: Vec<Argument>,
    },
    
    /// Transfer objects to an address
    TransferObjects {
        objects: Vec<Argument>,
        address: Argument,
    },
    
    /// Publish a new Move package
    Publish {
        modules: Vec<Vec<u8>>,
        dependencies: Vec<String>,
    },
    
    /// Make a Move vector
    MakeMoveVec {
        type_: Option<TypeTag>,
        elements: Vec<Argument>,
    },
}

/// Transaction input
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum TransactionInput {
    /// Pure value (number, address, etc.)
    Pure { 
        value: Vec<u8>,  // BCS-encoded value
    },
    
    /// Object reference
    Object {
        #[serde(flatten)]
        object_ref: ObjectRef,
    },
}

/// Transaction kind - Programmable Transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgrammableTransaction {
    pub inputs: Vec<TransactionInput>,
    pub commands: Vec<Command>,
}

/// Gas budget and payment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasData {
    pub payment: Vec<ObjectRef>,  // Coins used for gas
    pub owner: String,             // Gas payer address
    pub price: u64,                // Gas price in MIST
    pub budget: u64,               // Gas budget in MIST
}

/// Complete transaction data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    pub kind: TransactionKind,
    pub sender: String,
    pub gas_data: GasData,
    pub expiration: TransactionExpiration,
}

/// Transaction kind wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum TransactionKind {
    ProgrammableTransaction(ProgrammableTransaction),
}

/// Transaction expiration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum TransactionExpiration {
    None,
    Epoch(u64),
}

/// Builder for Programmable Transaction Blocks
pub struct PtbBuilder {
    inputs: Vec<TransactionInput>,
    commands: Vec<Command>,
}

impl PtbBuilder {
    pub fn new() -> Self {
        Self {
            inputs: Vec::new(),
            commands: Vec::new(),
        }
    }
    
    /// Add a pure input (BCS-encoded value)
    pub fn add_pure_input(&mut self, value: Vec<u8>) -> Argument {
        let index = self.inputs.len() as u16;
        self.inputs.push(TransactionInput::Pure { value });
        Argument::Input { index }
    }
    
    /// Add an object input
    pub fn add_object_input(&mut self, object_ref: ObjectRef) -> Argument {
        let index = self.inputs.len() as u16;
        self.inputs.push(TransactionInput::Object { object_ref });
        Argument::Input { index }
    }
    
    /// Add a SplitCoins command
    pub fn split_coins(&mut self, coin: Argument, amounts: Vec<Argument>) -> Argument {
        let cmd_index = self.commands.len() as u16;
        self.commands.push(Command::SplitCoins { coin, amounts });
        Argument::Result { cmd_index }
    }
    
    /// Add a MergeCoins command
    pub fn merge_coins(&mut self, destination: Argument, sources: Vec<Argument>) {
        self.commands.push(Command::MergeCoins { destination, sources });
    }
    
    /// Add a MoveCall command
    pub fn move_call(
        &mut self,
        package: String,
        module: String,
        function: String,
        type_arguments: Vec<TypeTag>,
        arguments: Vec<Argument>,
    ) -> Argument {
        let cmd_index = self.commands.len() as u16;
        self.commands.push(Command::MoveCall {
            package,
            module,
            function,
            type_arguments,
            arguments,
        });
        Argument::Result { cmd_index }
    }
    
    /// Add a TransferObjects command
    pub fn transfer_objects(&mut self, objects: Vec<Argument>, address: Argument) {
        self.commands.push(Command::TransferObjects { objects, address });
    }
    
    /// Build the final ProgrammableTransaction
    pub fn build(self) -> ProgrammableTransaction {
        ProgrammableTransaction {
            inputs: self.inputs,
            commands: self.commands,
        }
    }
    
    /// Build complete TransactionData
    pub fn build_transaction(
        self,
        sender: String,
        gas_payment: Vec<ObjectRef>,
        gas_price: u64,
        gas_budget: u64,
    ) -> TransactionData {
        TransactionData {
            kind: TransactionKind::ProgrammableTransaction(self.build()),
            sender: sender.clone(),
            gas_data: GasData {
                payment: gas_payment,
                owner: sender,
                price: gas_price,
                budget: gas_budget,
            },
            expiration: TransactionExpiration::None,
        }
    }
}

impl Default for PtbBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to encode pure values using BCS
pub mod bcs_helpers {
    use super::*;
    
    /// Encode a u64 value
    pub fn encode_u64(value: u64) -> Result<Vec<u8>, ExecutionError> {
        bcs::to_bytes(&value)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to encode u64: {}", e)))
    }
    
    /// Encode a u128 value
    pub fn encode_u128(value: u128) -> Result<Vec<u8>, ExecutionError> {
        bcs::to_bytes(&value)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to encode u128: {}", e)))
    }
    
    /// Encode a boolean
    pub fn encode_bool(value: bool) -> Result<Vec<u8>, ExecutionError> {
        bcs::to_bytes(&value)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to encode bool: {}", e)))
    }
    
    /// Encode an address (as 32-byte array)
    pub fn encode_address(address: &str) -> Result<Vec<u8>, ExecutionError> {
        // Remove 0x prefix
        let addr = address.strip_prefix("0x").unwrap_or(address);
        
        // Decode hex to bytes
        let bytes = hex::decode(addr)
            .map_err(|e| ExecutionError::SerializationError(format!("Invalid address hex: {}", e)))?;
        
        if bytes.len() != 32 {
            return Err(ExecutionError::Validation(
                format!("Address must be 32 bytes, got {}", bytes.len())
            ));
        }
        
        // BCS encode the 32-byte array
        bcs::to_bytes(&bytes)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to encode address: {}", e)))
    }
    
    /// Encode a vector of bytes
    pub fn encode_bytes(bytes: &[u8]) -> Result<Vec<u8>, ExecutionError> {
        bcs::to_bytes(bytes)
            .map_err(|e| ExecutionError::SerializationError(format!("Failed to encode bytes: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ptb_builder() {
        let mut builder = PtbBuilder::new();
        
        // Add inputs
        let amount = builder.add_pure_input(vec![100, 0, 0, 0, 0, 0, 0, 0]); // u64: 100
        let recipient = builder.add_pure_input(vec![0u8; 32]); // Address
        
        // Split gas coin
        let split_result = builder.split_coins(Argument::GasCoin, vec![amount]);
        
        // Transfer split coin
        builder.transfer_objects(vec![split_result], recipient);
        
        let ptb = builder.build();
        
        assert_eq!(ptb.inputs.len(), 2);
        assert_eq!(ptb.commands.len(), 2);
    }
    
    #[test]
    fn test_bcs_encoding() {
        // Test u64 encoding
        let encoded = bcs_helpers::encode_u64(1000).unwrap();
        assert!(!encoded.is_empty());
        
        // Test address encoding
        let addr = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let encoded = bcs_helpers::encode_address(addr).unwrap();
        assert!(!encoded.is_empty());
    }
}
