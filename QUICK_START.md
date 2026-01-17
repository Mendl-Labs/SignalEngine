# Phase 3: Quick Start Card

## 🚀 Run Tests in 3 Steps

### Step 1: Setup (One-time)
```powershell
# Get a private key (choose one method):

# Option A: Generate new key
sui client new-address ed25519
sui keytool export --key-identity YOUR_ADDRESS

# Option B: Use existing key
# (Find in ~/.sui/sui_config/sui.keystore)

# Set environment variable
$env:SUI_PRIVATE_KEY = "your_private_key_hex_here"
```

### Step 2: Fund Wallet
```bash
# Get your address
sui client active-address

# Fund from faucet
sui client faucet --address YOUR_ADDRESS

# Or use web: https://faucet.devnet.sui.io/
```

### Step 3: Run Tests
```powershell
cd C:\Users\ikenn\Projects\TradingPlatform\SignalEngine
.\scripts\run_devnet_tests.ps1
```

**That's it!** The script handles everything else.

---

## 🔧 Manual Testing

```bash
# Test everything (recommended)
cargo run --example test_dex_devnet -- both

# Test Cetus only (AMM)
cargo run --example test_dex_devnet -- cetus

# Test DeepBook only (CLOB)
cargo run --example test_dex_devnet -- deepbook
```

---

## ✅ Expected Results

### Success Looks Like:
```
📊 TEST SUMMARY
   ✅ Cetus Quote
   ✅ Cetus Swap - https://suiscan.xyz/devnet/tx/...
   ✅ DeepBook Init
   ✅ DeepBook Limit Order - https://suiscan.xyz/devnet/tx/...

   Tests Passed: 4/4
   Total Gas Used: ~0.08 SUI
   Total Latency: ~900ms

   🎉 ALL TESTS PASSED! Ready for mainnet.
```

### Key Metrics:
- **Cetus Swap**: ~0.05 SUI gas, ~450ms latency
- **DeepBook Order**: ~0.03 SUI gas, ~420ms latency
- **Slippage**: <50 bps (protected)
- **Status**: Filled (Cetus) / Pending (DeepBook)

---

## 🔍 View Transactions

After tests, check on SuiScan:
```
https://suiscan.xyz/devnet/tx/YOUR_TX_HASH
```

Look for:
- ✅ Status: Success
- ✅ Gas used matches estimate
- ✅ Events emitted
- ✅ Coins transferred

---

## 🐛 Quick Troubleshooting

| Problem | Fix |
|---------|-----|
| "Private key not set" | `$env:SUI_PRIVATE_KEY = "your_key"` |
| "Insufficient balance" | `sui client faucet` |
| "Pool not found" | Pool addresses may need updating |
| "Transaction failed" | Check SuiScan for details |

---

## 📁 Key Files

```
SignalEngine/
├── scripts/
│   └── run_devnet_tests.ps1          # Automated setup
├── crates/executionhandler/examples/
│   └── test_dex_devnet.rs            # Test suite (550 lines)
├── DEVNET_TESTING_GUIDE.md           # Full manual
├── PHASE3_STATUS.md                  # Detailed status
└── DEX_PHASE2_COMPLETE.md            # Implementation details
```

---

## 📊 What Gets Tested

### Cetus (AMM)
1. ✅ Quote: 0.1 SUI → USDC price
2. ✅ Swap: 0.01 SUI → USDC (real transaction)
   - Slippage protection
   - Gas cost tracking
   - Latency measurement

### DeepBook (CLOB)
1. ✅ Initialization
2. ✅ Limit Order: Buy 0.01 SUI @ market * 1.01
   - Order placement
   - Gas optimization
   - Status tracking

---

## ⏱️ Time Required

- **Setup**: 15-30 minutes (one-time)
- **Testing**: 5-10 minutes
- **Review**: 5-10 minutes
- **Total**: ~30-45 minutes

---

## 🎯 Success Criteria

Phase 3 is complete when:
- ✅ All 4 tests pass
- ✅ Gas costs <0.1 SUI total
- ✅ Latency <1 second each
- ✅ Transactions verified on SuiScan
- ✅ Slippage protection confirmed

---

## 📞 Need Help?

1. Check `DEVNET_TESTING_GUIDE.md` (full manual)
2. Review error messages carefully
3. Check transaction on SuiScan
4. Verify wallet balance: `sui client gas`

---

## ⏭️ After Testing

Once all tests pass:

1. **Review Results**
   - Check gas costs
   - Verify slippage
   - Confirm latency

2. **Next Phase: Mainnet**
   - Update network config
   - Start with 0.01 SUI
   - Monitor closely

---

## 🔗 Resources

- **Devnet Faucet**: https://faucet.devnet.sui.io/
- **Explorer**: https://suiscan.xyz/devnet
- **SUI Docs**: https://docs.sui.io/
- **Cetus**: https://cetus.zone/
- **DeepBook**: https://docs.sui.io/standards/deepbook

---

**Ready?** Run: `.\scripts\run_devnet_tests.ps1`
