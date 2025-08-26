# SignalEngine Simple Deployment Script
param(
    [Parameter(Mandatory=$false)]
    [string]$Environment = "dev",
    
    [Parameter(Mandatory=$false)]
    [string]$ReleaseName = "signal-engine-$Environment",
    
    [Parameter(Mandatory=$false)]
    [string]$Namespace = "signal-engine"
)

Write-Host "🚀 SignalEngine Deployment" -ForegroundColor Green
Write-Host "Environment: $Environment" -ForegroundColor Yellow
Write-Host "Release: $ReleaseName" -ForegroundColor Yellow
Write-Host "Namespace: $Namespace" -ForegroundColor Yellow

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$helmDir = Join-Path $scriptDir "k8s\signal-engine"

# Check Helm chart
if (-not (Test-Path $helmDir)) {
    Write-Error "❌ Helm chart not found: $helmDir"
    exit 1
}

# Get credentials
Write-Host "`n🔐 Enter API Credentials:" -ForegroundColor Cyan
$apiKey = Read-Host "Kraken API Key"
$secretKeySecure = Read-Host "Kraken Secret Key" -AsSecureString
$secretKey = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto([System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secretKeySecure))

Write-Host "`n🚀 Deploying..." -ForegroundColor Green

try {
    $helmArgs = @(
        "install", $ReleaseName, $helmDir,
        "--namespace", $Namespace,
        "--create-namespace",
        "--set-string", "secrets.kraken.apiKey=$apiKey",
        "--set-string", "secrets.kraken.secretKey=$secretKey",
        "--set", "global.environment=$Environment"
    )
    
    & helm $helmArgs
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✅ Deployment successful!" -ForegroundColor Green
        Write-Host "`nNext steps:" -ForegroundColor Yellow
        Write-Host "  helm status $ReleaseName -n $Namespace" -ForegroundColor White
        Write-Host "  kubectl get pods -namespace $Namespace" -ForegroundColor White
    } else {
        Write-Error "❌ Deployment failed"
        exit 1
    }
} catch {
    Write-Error "❌ Error: $($_.Exception.Message)"
    exit 1
} finally {
    # Clear sensitive data
    $apiKey = $null
    $secretKey = $null
    $secretKeySecure.Dispose()
}

Write-Host "`n🔒 Credentials handled securely!" -ForegroundColor Magenta
