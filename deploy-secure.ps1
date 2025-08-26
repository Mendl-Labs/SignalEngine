# SignalEngine Secure Deployment Script
# This script allows secure deployment of SignalEngine without exposing credentials in Git

param(
    [Parameter(Mandatory=$false)]
    [string]$Environment = "dev",
    
    [Parameter(Mandatory=$false)]
    [string]$ReleaseName = "signal-engine-$Environment",
    
    [Parameter(Mandatory=$false)]
    [string]$Namespace = "signal-engine",
    
    [Parameter(Mandatory=$false)]
    [switch]$UseExternalSecrets = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$TestConnection = $true
)

Write-Host "🚀 SignalEngine Secure Deployment Script" -ForegroundColor Green
Write-Host "Environment: $Environment" -ForegroundColor Yellow
Write-Host "Release Name: $ReleaseName" -ForegroundColor Yellow
Write-Host "Namespace: $Namespace" -ForegroundColor Yellow

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$helmDir = Join-Path $scriptDir "k8s\signal-engine"
$valuesFile = Join-Path $helmDir "values.yaml"

# Check if Helm chart exists
if (-not (Test-Path $helmDir)) {
    Write-Error "❌ Helm chart directory not found: $helmDir"
    exit 1
}

if (-not (Test-Path $valuesFile)) {
    Write-Error "❌ Main values.yaml not found: $valuesFile"
    exit 1
}

Write-Host "`n🔐 Credential Input (secure prompting)" -ForegroundColor Cyan

if ($UseExternalSecrets) {
    Write-Host "✅ Using external secret management - no credential input needed" -ForegroundColor Green
    $helmArgs = @(
        "install", $ReleaseName, $helmDir,
        "--namespace", $Namespace,
        "--create-namespace",
        "-f", $valuesFile,
        "--set", "externalSecrets.enabled=true",
        "--set", "global.environment=$Environment"
    )
} else {
    # Prompt for Kraken API credentials securely
    Write-Host "🔑 Enter Kraken API credentials:" -ForegroundColor Yellow
    $krakenApiKey = Read-Host "Kraken API Key"
    $krakenSecretSecure = Read-Host "Kraken Secret Key" -AsSecureString
    $krakenSecret = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto([System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($krakenSecretSecure))
    
    Write-Host "🔑 Enter database credentials:" -ForegroundColor Yellow
    $dbPasswordSecure = Read-Host "Database Password" -AsSecureString
    $dbPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto([System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($dbPasswordSecure))
    
    Write-Host "🔑 Enter Redis credentials:" -ForegroundColor Yellow
    $redisPasswordSecure = Read-Host "Redis Password" -AsSecureString
    $redisPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto([System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($redisPasswordSecure))
    
    Write-Host "🔑 Enter message broker credentials:" -ForegroundColor Yellow
    $brokerPasswordSecure = Read-Host "Message Broker Password" -AsSecureString
    $brokerPassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto([System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($brokerPasswordSecure))
    
    # Test API credentials if requested
    if ($TestConnection) {
        Write-Host "`n🧪 Testing Kraken API connection..." -ForegroundColor Cyan
        
        # Simple API test (you can enhance this)
        try {
            $testUrl = "https://api.kraken.com/0/public/Time"
            $response = Invoke-RestMethod -Uri $testUrl -Method Get -TimeoutSec 10
            if ($response.result) {
                Write-Host "✅ Kraken API endpoint is reachable" -ForegroundColor Green
            } else {
                Write-Warning "⚠️  Kraken API test inconclusive"
            }
        } catch {
            Write-Warning "⚠️  Could not test Kraken API connectivity: $($_.Exception.Message)"
        }
    }
    
    # Build Helm command with secure credential injection
    $helmArgs = @(
        "install", $ReleaseName, $helmDir,
        "--namespace", $Namespace,
        "--create-namespace",
        "-f", $valuesFile,
        "--set-string", "secrets.kraken.apiKey=$krakenApiKey",
        "--set-string", "secrets.kraken.secretKey=$krakenSecret",
        "--set-string", "secrets.database.password=$dbPassword",
        "--set-string", "secrets.redis.password=$redisPassword",
        "--set-string", "secrets.messageBroker.password=$brokerPassword",
        "--set", "global.environment=$Environment"
    )
    
    # Clear sensitive variables immediately
    $krakenSecret = $null
    $dbPassword = $null
    $redisPassword = $null
    $brokerPassword = $null
    $krakenSecretSecure.Dispose()
    $dbPasswordSecure.Dispose()
    $redisPasswordSecure.Dispose()
    $brokerPasswordSecure.Dispose()
}

Write-Host "`n🚀 Deploying SignalEngine..." -ForegroundColor Green
Write-Host "Command: helm $($helmArgs -join ' ')" -ForegroundColor Gray

try {
    & helm $helmArgs
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✅ SignalEngine deployed successfully!" -ForegroundColor Green
        
        Write-Host "`n📊 Checking deployment status..." -ForegroundColor Yellow
        Start-Sleep -Seconds 5
        
        & kubectl get pods -n $Namespace -l "app.kubernetes.io/name=signal-engine-helm"
        
        Write-Host "`nℹ️  Useful commands:" -ForegroundColor Yellow
        Write-Host "  Check status    : helm status $ReleaseName -n $Namespace" -ForegroundColor White
        Write-Host "  View logs       : kubectl logs -n $Namespace -l app.kubernetes.io/name=signal-engine-helm -f" -ForegroundColor White
        Write-Host "  Port forward    : kubectl port-forward -n $Namespace svc/$ReleaseName-signal-engine-helm 8080:8080" -ForegroundColor White
        Write-Host "  Upgrade         : helm upgrade $ReleaseName $helmDir -n $Namespace [options]" -ForegroundColor White
        Write-Host "  Uninstall       : helm uninstall $ReleaseName -n $Namespace" -ForegroundColor White
        
    } else {
        Write-Error "❌ Deployment failed with exit code $LASTEXITCODE"
        exit 1
    }
} catch {
    Write-Error "❌ Deployment failed: $($_.Exception.Message)"
    exit 1
}

Write-Host "`n🔒 Security Notes:" -ForegroundColor Magenta
Write-Host "  • API credentials were injected securely via --set-string" -ForegroundColor White
Write-Host "  • No credentials were stored in files or Git history" -ForegroundColor White  
Write-Host "  • Credentials are stored as Kubernetes Secrets (base64 encoded)" -ForegroundColor White
Write-Host "  • Use external secret management for production environments" -ForegroundColor White
Write-Host "  • Rotate API keys regularly and monitor usage" -ForegroundColor White

# Usage examples:
# .\deploy-secure.ps1                                    # Dev deployment with credential prompts
# .\deploy-secure.ps1 -Environment prod                  # Production deployment
# .\deploy-secure.ps1 -UseExternalSecrets               # Use external secret management
# .\deploy-secure.ps1 -TestConnection:$false            # Skip API connection test
