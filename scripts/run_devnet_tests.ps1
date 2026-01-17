# Phase 3 Devnet Testing Script
# 
# This script helps set up and run comprehensive devnet tests for both Cetus and DeepBook

Write-Host "`n" -NoNewline
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  PHASE 3: DEX DEVNET TESTING SETUP" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check if SUI CLI is installed
Write-Host "1. Checking SUI CLI installation..." -ForegroundColor Yellow
$suiInstalled = Get-Command sui -ErrorAction SilentlyContinue
if ($suiInstalled) {
    Write-Host "   ✓ SUI CLI found" -ForegroundColor Green
    $suiVersion = sui --version 2>&1
    Write-Host "   Version: $suiVersion" -ForegroundColor Gray
} else {
    Write-Host "   ✗ SUI CLI not found" -ForegroundColor Red
    Write-Host ""
    Write-Host "   To install SUI CLI:" -ForegroundColor Yellow
    Write-Host "   cargo install --locked --git https://github.com/MystenLabs/sui.git sui" -ForegroundColor White
    Write-Host ""
    $install = Read-Host "   Install now? (y/n)"
    if ($install -eq "y") {
        Write-Host "   Installing SUI CLI (this may take 10-15 minutes)..." -ForegroundColor Yellow
        cargo install --locked --git https://github.com/MystenLabs/sui.git sui
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✓ SUI CLI installed successfully" -ForegroundColor Green
        } else {
            Write-Host "   ✗ Installation failed" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "   Skipping SUI CLI installation" -ForegroundColor Gray
        Write-Host "   Note: You'll need to manually manage your wallet" -ForegroundColor Yellow
    }
}

Write-Host ""

# Step 2: Check for private key
Write-Host "2. Checking environment configuration..." -ForegroundColor Yellow
if ($env:SUI_PRIVATE_KEY) {
    Write-Host "   ✓ SUI_PRIVATE_KEY is set" -ForegroundColor Green
    $keyPreview = $env:SUI_PRIVATE_KEY.Substring(0, [Math]::Min(16, $env:SUI_PRIVATE_KEY.Length))
    Write-Host "   Key: $keyPreview..." -ForegroundColor Gray
} else {
    Write-Host "   ✗ SUI_PRIVATE_KEY not set" -ForegroundColor Red
    Write-Host ""
    Write-Host "   You need a SUI devnet private key to run tests." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "   Options:" -ForegroundColor White
    Write-Host "   A) Generate new key with SUI CLI" -ForegroundColor White
    Write-Host "   B) Use existing key" -ForegroundColor White
    Write-Host ""
    $choice = Read-Host "   Choose option (a/b)"
    
    if ($choice -eq "a" -and $suiInstalled) {
        Write-Host ""
        Write-Host "   Generating new ED25519 key..." -ForegroundColor Yellow
        $output = sui client new-address ed25519 2>&1 | Out-String
        Write-Host $output -ForegroundColor Gray
        
        Write-Host ""
        Write-Host "   ⚠️  IMPORTANT: Save your recovery phrase securely!" -ForegroundColor Red
        Write-Host ""
        Write-Host "   To export the private key:" -ForegroundColor Yellow
        Write-Host "   1. Find your keystore: sui client addresses" -ForegroundColor White
        Write-Host "   2. Export key: sui keytool export --key-identity YOUR_ADDRESS" -ForegroundColor White
        Write-Host ""
        
        Read-Host "   Press Enter when you have your private key ready"
    }
    
    Write-Host ""
    $privateKey = Read-Host "   Enter your private key (hex format)"
    
    if ($privateKey) {
        $env:SUI_PRIVATE_KEY = $privateKey
        Write-Host "   ✓ Private key set for this session" -ForegroundColor Green
        Write-Host ""
        Write-Host "   To set permanently:" -ForegroundColor Yellow
        Write-Host "   [System.Environment]::SetEnvironmentVariable('SUI_PRIVATE_KEY', '$privateKey', 'User')" -ForegroundColor White
    } else {
        Write-Host "   ✗ No private key provided" -ForegroundColor Red
        exit 1
    }
}

Write-Host ""

# Step 3: Check wallet balance
Write-Host "3. Checking wallet balance..." -ForegroundColor Yellow
if ($suiInstalled) {
    $balance = sui client gas --json 2>&1 | ConvertFrom-Json -ErrorAction SilentlyContinue
    if ($balance) {
        $totalBalance = ($balance | Measure-Object -Property balance -Sum).Sum
        $suiBalance = $totalBalance / 1000000000
        
        if ($suiBalance -gt 0.5) {
            Write-Host "   ✓ Balance: $suiBalance SUI" -ForegroundColor Green
            Write-Host "   Sufficient for testing" -ForegroundColor Gray
        } elseif ($suiBalance -gt 0.1) {
            Write-Host "   ⚠️  Balance: $suiBalance SUI" -ForegroundColor Yellow
            Write-Host "   Recommended to have at least 0.5 SUI" -ForegroundColor Gray
        } else {
            Write-Host "   ✗ Balance: $suiBalance SUI" -ForegroundColor Red
            Write-Host "   Insufficient for testing" -ForegroundColor Gray
            
            Write-Host ""
            Write-Host "   To fund your wallet:" -ForegroundColor Yellow
            $address = sui client active-address 2>&1
            Write-Host "   sui client faucet --address $address" -ForegroundColor White
            Write-Host ""
            Write-Host "   Or use the web faucet:" -ForegroundColor Yellow
            Write-Host "   https://faucet.devnet.sui.io/" -ForegroundColor White
            Write-Host ""
            
            $fund = Read-Host "   Request faucet now? (y/n)"
            if ($fund -eq "y") {
                Write-Host "   Requesting devnet SUI..." -ForegroundColor Yellow
                sui client faucet
                Start-Sleep -Seconds 5
                
                # Check balance again
                $newBalance = sui client gas --json 2>&1 | ConvertFrom-Json
                $newTotal = ($newBalance | Measure-Object -Property balance -Sum).Sum
                $newSui = $newTotal / 1000000000
                Write-Host "   New balance: $newSui SUI" -ForegroundColor Green
            }
        }
    }
}

Write-Host ""

# Step 4: Build the project
Write-Host "4. Building test binary..." -ForegroundColor Yellow
Write-Host "   This may take a few minutes..." -ForegroundColor Gray
$buildOutput = cargo build --example test_dex_devnet 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "   ✓ Build successful" -ForegroundColor Green
} else {
    Write-Host "   ✗ Build failed" -ForegroundColor Red
    Write-Host ""
    Write-Host $buildOutput -ForegroundColor Red
    exit 1
}

Write-Host ""

# Step 5: Run tests
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  READY TO RUN TESTS" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Test options:" -ForegroundColor Yellow
Write-Host "  1. Test Cetus only (AMM swaps)" -ForegroundColor White
Write-Host "  2. Test DeepBook only (Limit orders)" -ForegroundColor White
Write-Host "  3. Test both (Full suite)" -ForegroundColor White
Write-Host "  4. Exit" -ForegroundColor White
Write-Host ""

$testChoice = Read-Host "Choose option (1-4)"

switch ($testChoice) {
    "1" {
        Write-Host ""
        Write-Host "Running Cetus tests..." -ForegroundColor Cyan
        cargo run --example test_dex_devnet -- cetus
    }
    "2" {
        Write-Host ""
        Write-Host "Running DeepBook tests..." -ForegroundColor Cyan
        cargo run --example test_dex_devnet -- deepbook
    }
    "3" {
        Write-Host ""
        Write-Host "Running full test suite..." -ForegroundColor Cyan
        cargo run --example test_dex_devnet -- both
    }
    "4" {
        Write-Host "Exiting..." -ForegroundColor Gray
        exit 0
    }
    default {
        Write-Host "Invalid option. Running full test suite..." -ForegroundColor Yellow
        cargo run --example test_dex_devnet -- both
    }
}

Write-Host ""
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "  TESTING COMPLETE" -ForegroundColor Cyan
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""

# Step 6: Post-test summary
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  ✓ Review transaction links on SuiScan" -ForegroundColor White
Write-Host "  ✓ Check gas usage and costs" -ForegroundColor White
Write-Host "  ✓ Verify slippage protection worked" -ForegroundColor White
Write-Host "  ✓ Monitor DeepBook limit orders" -ForegroundColor White
Write-Host ""
Write-Host "For mainnet testing:" -ForegroundColor Yellow
Write-Host "  1. Use small amounts (0.01-0.1 SUI)" -ForegroundColor White
Write-Host "  2. Change network to BlockchainNetwork::Sui" -ForegroundColor White
Write-Host "  3. Update RPC URL to mainnet" -ForegroundColor White
Write-Host "  4. Verify pool addresses for mainnet" -ForegroundColor White
Write-Host ""
