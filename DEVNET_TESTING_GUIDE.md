# Phase 3: Devnet Testing Guide

## Quick Start

### Prerequisites
1. **SUI CLI** (optional but recommended)
   ```bash
   cargo install --locked --git https://github.com/MystenLabs/sui.git sui
   ```

2. **Devnet Wallet with Funds**
   ```bash
   # Generate new wallet
   sui client new-address ed25519
   
   # Get your address
   sui client active-address
   
   # Fund from faucet
   sui client faucet --address YOUR_ADDRESS
   
   # Or use web faucet: https://faucet.devnet.sui.io/
   ```

3. **Export Private Key**
   ```bash
   sui keytool export --key-identity YOUR_ADDRESS
   ```

4. **Set Environment Variable**
   ```powershell
   # PowerShell (current session)
   $env:SUI_PRIVATE_KEY = "your_private_key_hex"
   
   # PowerShell (permanent)
   [System.Environment]::SetEnvironmentVariable('SUI_PRIVATE_KEY', 'your_key', 'User')
   
   # Bash
   export SUI_PRIVATE_KEY="your_private_key_hex"
   ```

### Running Tests

#### Option 1: Automated Setup (Recommended)
```powershell
cd SignalEngine
.\scripts\run_devnet_tests.ps1
```
This script will:
- Check SUI CLI installation
- Verify wallet setup and balance
- Build the test binary
- Run interactive tests

#### Option 2: Manual Execution
```bash
# Test Cetus only
cargo run --example test_dex_devnet -- cetus

# Test DeepBook only
cargo run --example test_dex_devnet -- deepbook

# Test both (full suite)
cargo run --example test_dex_devnet -- both
```

## Test Cases

### Cetus (AMM) Tests

#### Test 1: Quote Retrieval
**Purpose**: Verify pool queries and price calculations

**Expected Results**:
- ✅ Successfully queries SUI/USDC pool
- ✅ Returns expected output amount
- ✅ Calculates minimum output with slippage
- ✅ Shows price impact in basis points
- ✅ Estimates gas cost

**Success Criteria**:
- Quote completes in <2 seconds
- Price seems reasonable (~$1-3 per SUI on devnet)
- Minimum output = expected * (1 - slippage_bps/10000)

#### Test 2: Swap Execution
**Purpose**: Execute real swap transaction with slippage protection

**Parameters**:
- Amount: 0.01 SUI (very small for safety)
- Pair: SUI → USDC
- Slippage: 50 bps (0.5%)

**Expected Results**:
- ✅ Transaction completes successfully
- ✅ Returns transaction hash
- ✅ Status: `Filled`
- ✅ Gas used: ~50M MIST (0.05 SUI)
- ✅ Actual slippage ≤ 50 bps
- ✅ Latency: 400-600ms

**Verification**:
1. Check transaction on SuiScan: `https://suiscan.xyz/devnet/tx/TX_HASH`
2. Verify swap amount matches input
3. Confirm slippage protection worked
4. Check gas cost is reasonable

**Troubleshooting**:
- **Error: Pool not found** → Pool address may be incorrect for devnet
- **Error: Insufficient balance** → Fund wallet with faucet
- **Error: Transaction failed** → Check gas balance, may need more SUI
- **High slippage** → Pool may have low liquidity on devnet

### DeepBook (CLOB) Tests

#### Test 1: Initialization
**Purpose**: Verify connector setup and wallet configuration

**Expected Results**:
- ✅ Connector initializes successfully
- ✅ Network set to devnet
- ✅ Wallet address correct

#### Test 2: Limit Order Placement
**Purpose**: Place a real limit order in DeepBook order book

**Parameters**:
- Side: BUY
- Quantity: 0.01 SUI
- Price: Market price * 1.01 (slightly above to ensure fill)

**Expected Results**:
- ✅ Transaction completes successfully
- ✅ Returns transaction hash and order ID
- ✅ Status: `Pending` (waiting for fill)
- ✅ Gas used: ~30M MIST (0.03 SUI)
- ✅ Order visible in DeepBook pool
- ✅ Latency: 400-600ms

**Verification**:
1. Check transaction on SuiScan
2. Verify order parameters (price, quantity, side)
3. Confirm order is in pending state
4. Monitor for order fill

**Order Lifecycle**:
- **Pending**: Order submitted, waiting for matching
- **Filled**: Order matched and executed
- **Cancelled**: Order manually cancelled

**Troubleshooting**:
- **Error: Pool not found** → Pool ID may be incorrect for devnet
- **Error: Below minimum lot size** → DeepBook has minimum order sizes
- **Order never fills** → Price may be too far from market
- **High gas cost** → Normal for first order, subsequent orders cheaper

## Expected Outcomes

### Successful Test Run

```
================================================================================
🧪 PHASE 3: DEX DEVNET TESTING
================================================================================

✓ Private key loaded from environment

📱 Setting up SUI Wallet
   ✓ Wallet created
   Network: Devnet
   RPC: https://fullnode.devnet.sui.io:443
   Address: 0xabc...123

💰 Checking Wallet Balance
   Balance: 1.5 SUI (1500000000 MIST)

================================================================================
🔵 Testing CETUS (AMM)
================================================================================

1️⃣  Initializing Cetus Connector
   ✓ Cetus connector initialized

2️⃣  Testing Quote (SUI → USDC)
   Query: 0.1 SUI → USDC
   ✓ Quote received:
     Expected output: 0.195 USDC
     Minimum output: 0.194 USDC
     Price: 1.95 USDC per SUI
     Price impact: 5 bps
     Route: SUI → USDC
     Est. gas: 50000 MIST

3️⃣  Testing Swap Execution
   ⚠️  EXECUTING REAL TRANSACTION
   Amount: 0.01 SUI → USDC (small test)

   ✅ SWAP SUCCESSFUL!
   TX Hash: BqG8xK...
   Status: Filled
   Filled: 0.01 SUI
   Avg Price: 1.95 USDC/SUI
   Total Fees: $0.000585
   Gas Used: 48234567 MIST (0.048234567 SUI)
   Slippage: 12 bps (limit: 50 bps)
   Latency: 456ms
   Confirmations: 1

================================================================================
📗 Testing DEEPBOOK (CLOB)
================================================================================

1️⃣  Initializing DeepBook Connector
   ✓ DeepBook connector initialized

2️⃣  Testing Limit Order Placement
   ⚠️  EXECUTING REAL TRANSACTION
   Getting price reference...
   Reference price: 1.95 USDC/SUI

   Placing BUY limit order:
   Quantity: 0.01 SUI
   Price: $1.9695 USDC/SUI

   ✅ LIMIT ORDER PLACED!
   TX Hash: DpK2yL...
   Order ID: deepbook_1700...
   Status: Pending
   Side: BUY
   Quantity: 0.01 SUI
   Limit Price: $1.9695 USDC
   Gas Used: 29876543 MIST (0.029876543 SUI)
   Latency: 423ms

   📌 Order Status: PENDING (waiting for fill)
   The order is now in the DeepBook order book.
   It will fill when market price reaches $1.9695

================================================================================
📊 TEST SUMMARY
================================================================================
   ✅ Cetus Quote
   ✅ Cetus Swap - https://suiscan.xyz/devnet/tx/BqG8xK...
   ✅ DeepBook Init
   ✅ DeepBook Limit Order - https://suiscan.xyz/devnet/tx/DpK2yL...

   Tests Passed: 4/4
   Total Gas Used: 78111110 MIST (0.07811111 SUI)
   Total Latency: 879ms

   🎉 ALL TESTS PASSED! Ready for mainnet.
================================================================================
```

### Performance Targets

| Metric | Target | Typical |
|--------|--------|---------|
| Cetus Quote Latency | <2s | ~500ms |
| Cetus Swap Latency | <1s | ~450ms |
| DeepBook Order Latency | <1s | ~420ms |
| Cetus Gas Cost | <0.1 SUI | ~0.05 SUI |
| DeepBook Gas Cost | <0.05 SUI | ~0.03 SUI |
| Slippage (Cetus) | ≤50 bps | ~10-20 bps |
| Finality | <1s | ~400ms |

## Post-Test Verification

### 1. Transaction Analysis
Visit SuiScan to review each transaction:
```
https://suiscan.xyz/devnet/tx/YOUR_TX_HASH
```

**Check**:
- ✅ Transaction status: Success
- ✅ Gas used matches estimate
- ✅ Events emitted (Swap, OrderPlaced, etc.)
- ✅ Object changes (coins transferred)

### 2. Cetus Swap Verification
- Compare input amount vs actual swap
- Verify slippage percentage
- Check fee calculation (typically 30 bps)
- Confirm output tokens received

### 3. DeepBook Order Verification
- Check order is in pool's order book
- Verify price and quantity
- Monitor for fill events
- Test cancellation if needed

### 4. Gas Cost Analysis
```
Cetus Swap:
- Computation cost: ~45-55M MIST
- Storage cost: ~1-2M MIST
- Total: ~50M MIST (0.05 SUI)

DeepBook Order:
- Computation cost: ~25-35M MIST
- Storage cost: ~1-2M MIST
- Total: ~30M MIST (0.03 SUI)
```

## Troubleshooting

### Common Issues

#### Issue 1: "No pool found"
**Cause**: Pool address incorrect for devnet

**Solution**:
1. Check `crates/executionhandler/src/exchanges/dex/cetus_constants.rs`
2. Verify devnet pool addresses
3. Query actual pools: `sui client call --package CETUS_PKG --module pool --function get_pool`

#### Issue 2: "Insufficient balance"
**Cause**: Not enough SUI for swap + gas

**Solution**:
```bash
sui client faucet --address YOUR_ADDRESS
```
Wait 30 seconds and check:
```bash
sui client gas
```

#### Issue 3: "Transaction failed"
**Cause**: Various reasons (gas, package mismatch, etc.)

**Debug Steps**:
1. Check error message in terminal
2. View transaction on SuiScan
3. Verify package addresses match network
4. Ensure coin types are correct
5. Check pool liquidity

#### Issue 4: High slippage
**Cause**: Low liquidity on devnet pools

**Solution**:
- Reduce swap amount (use 0.001 SUI)
- Increase slippage tolerance temporarily
- Use mainnet for production (better liquidity)

#### Issue 5: DeepBook order never fills
**Cause**: Limit price too far from market

**Solution**:
- Check current market price
- Place order closer to market (within 1%)
- Or cancel and place new order

## Next Steps After Successful Tests

### Phase 4: Mainnet Preparation

1. **Review Gas Costs**
   - Actual costs match estimates? ✓
   - Optimizations needed? ✓
   - Gas budget sufficient? ✓

2. **Verify Slippage Protection**
   - All swaps within tolerance? ✓
   - Protection triggered correctly? ✓
   - Settings appropriate? ✓

3. **Test Edge Cases**
   - Very small amounts (0.001 SUI)
   - Large amounts (1+ SUI) if sufficient funds
   - Multiple rapid transactions
   - Network congestion handling

4. **Update Configuration**
   - Change network to `BlockchainNetwork::Sui`
   - Update RPC URL to mainnet
   - Verify all pool addresses for mainnet
   - Test with 0.01 SUI first on mainnet

5. **Integration Testing**
   - Connect to Kraken HFT system
   - Test cross-venue arbitrage
   - Benchmark end-to-end latency
   - Monitor for any issues

6. **Production Deployment**
   - Start with small amounts
   - Monitor closely for first 24 hours
   - Scale up gradually
   - Set up alerts and monitoring

## Success Criteria

### Phase 3 Complete When:
- ✅ All 4 tests pass
- ✅ Gas costs reasonable (<0.1 SUI total)
- ✅ Latency <1 second
- ✅ Slippage protection works
- ✅ Transactions confirmed on SuiScan
- ✅ DeepBook orders visible in book

### Ready for Mainnet When:
- ✅ 10+ successful devnet tests
- ✅ No transaction failures
- ✅ Gas estimates accurate (±10%)
- ✅ All edge cases handled
- ✅ Error handling comprehensive
- ✅ Monitoring setup complete

## Resources

- **SUI Devnet Faucet**: https://faucet.devnet.sui.io/
- **SUI Explorer (Devnet)**: https://suiscan.xyz/devnet
- **Cetus Protocol**: https://cetus.zone/
- **DeepBook Docs**: https://docs.sui.io/standards/deepbook
- **SUI CLI Docs**: https://docs.sui.io/references/cli

## Support

If you encounter issues:
1. Check error messages carefully
2. Review transaction on SuiScan
3. Verify environment setup
4. Check wallet balance
5. Consult troubleshooting section above

For package/pool address issues, query the actual devnet:
```bash
# List all objects owned by address
sui client objects YOUR_ADDRESS

# Get object details
sui client object OBJECT_ID

# Query move package
sui client package PACKAGE_ADDRESS
```
