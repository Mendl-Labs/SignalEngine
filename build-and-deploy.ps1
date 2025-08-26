#!/usr/bin/env powershell
# SignalEngine Docker Build and Deploy Script
# This script builds the SignalEngine Docker image and updates the Kubernetes deployment

param(
    [Parameter(Mandatory=$false)]
    [string]$ImageTag = "latest",
    
    [Parameter(Mandatory=$false)]
    [string]$Registry = "",
    
    [Parameter(Mandatory=$false)]
    [switch]$Deploy,
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipBuild,
    
    [Parameter(Mandatory=$false)]
    [string]$Namespace = "signal-engine"
)

$ErrorActionPreference = "Stop"

# Colors for output
$Green = [Console]::ForegroundColor = 'Green'
$Red = [Console]::ForegroundColor = 'Red'
$Yellow = [Console]::ForegroundColor = 'Yellow'
$White = [Console]::ForegroundColor = 'White'

function Write-ColorOutput($ForegroundColor, $Message) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    Write-Output $Message
    $host.UI.RawUI.ForegroundColor = $fc
}

Write-ColorOutput Green "🚀 SignalEngine Docker Build and Deploy Script"
Write-ColorOutput Green "=================================================="

# Set image name
$ImageName = if ($Registry) { "$Registry/signal-engine:$ImageTag" } else { "signal-engine:$ImageTag" }

Write-ColorOutput Yellow "Configuration:"
Write-ColorOutput White "  Image: $ImageName"
Write-ColorOutput White "  Skip Build: $SkipBuild"
Write-ColorOutput White "  Deploy: $Deploy"
Write-ColorOutput White "  Namespace: $Namespace"
Write-Output ""

if (-not $SkipBuild) {
    Write-ColorOutput Yellow "🔨 Building SignalEngine Docker Image..."
    
    # Check if we're in the right directory
    if (-not (Test-Path "Dockerfile")) {
        Write-ColorOutput Red "❌ Error: Dockerfile not found. Make sure you're in the SignalEngine directory."
        exit 1
    }
    
    # Change to TradingPlatform root for build context
    Push-Location ".."
    
    try {
        Write-ColorOutput Yellow "Building from TradingPlatform root with full workspace context..."
        
        # Build the Docker image
        $buildCommand = "docker build -f SignalEngine/Dockerfile -t $ImageName . --no-cache"
        Write-ColorOutput White "Running: $buildCommand"
        
        & docker build -f SignalEngine/Dockerfile -t $ImageName . --no-cache
        
        if ($LASTEXITCODE -ne 0) {
            Write-ColorOutput Red "❌ Docker build failed!"
            exit 1
        }
        
        Write-ColorOutput Green "✅ Docker image built successfully: $ImageName"
        
        # Push to registry if specified
        if ($Registry) {
            Write-ColorOutput Yellow "📤 Pushing to registry..."
            & docker push $ImageName
            
            if ($LASTEXITCODE -ne 0) {
                Write-ColorOutput Red "❌ Docker push failed!"
                exit 1
            }
            
            Write-ColorOutput Green "✅ Image pushed to registry successfully"
        }
        
    } finally {
        Pop-Location
    }
}

if ($Deploy) {
    Write-ColorOutput Yellow "🚀 Deploying to Kubernetes..."
    
    # Check if kubectl is available
    try {
        & kubectl version --client --short | Out-Null
    } catch {
        Write-ColorOutput Red "❌ kubectl not found. Please install kubectl first."
        exit 1
    }
    
    # Check if Helm is available
    try {
        & helm version --short | Out-Null
    } catch {
        Write-ColorOutput Red "❌ Helm not found. Please install Helm first."
        exit 1
    }
    
    # Update values.yaml with the new image
    $valuesFile = "k8s/signal-engine/values.yaml"
    
    if (Test-Path $valuesFile) {
        Write-ColorOutput Yellow "📝 Updating Helm values with new image..."
        
        # Read the current values file
        $values = Get-Content $valuesFile -Raw
        
        # Update image repository and tag
        if ($Registry) {
            $imageRepo = "$Registry/signal-engine"
        } else {
            $imageRepo = "signal-engine"
        }
        
        # Replace image configuration
        $values = $values -replace 'repository:\s*(busybox|nginx|signal-engine)', "repository: $imageRepo"
        $values = $values -replace 'tag:\s*"[^"]*"', "tag: `"$ImageTag`""
        
        # Remove test command and args
        $values = $values -replace '# Command and args for test container[\s\S]*?args: \[.*?\]', ''
        $values = $values -replace 'command: \["/bin/sh"\]', '# command: []  # Use default from Dockerfile'
        $values = $values -replace 'args: \[.*?\]', '# args: []  # Use default from Dockerfile'
        
        # Write back to file
        Set-Content -Path $valuesFile -Value $values
        
        Write-ColorOutput Green "✅ Values file updated with image: $imageRepo:$ImageTag"
    }
    
    # Deploy with Helm
    Write-ColorOutput Yellow "🎯 Deploying with Helm..."
    
    $helmCommand = "helm upgrade signal-engine-dev k8s/signal-engine/ --namespace $Namespace --wait --timeout 600s"
    Write-ColorOutput White "Running: $helmCommand"
    
    & helm upgrade signal-engine-dev k8s/signal-engine/ --namespace $Namespace --wait --timeout 600s
    
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput Red "❌ Helm deployment failed!"
        exit 1
    }
    
    Write-ColorOutput Green "✅ SignalEngine deployed successfully!"
    
    # Show deployment status
    Write-ColorOutput Yellow "📊 Deployment Status:"
    & kubectl get pods -n $Namespace -l app.kubernetes.io/name=signal-engine-helm
    
    Write-ColorOutput Yellow "🔍 To check logs:"
    Write-ColorOutput White "kubectl logs -l app.kubernetes.io/name=signal-engine-helm -n $Namespace -f"
    
    Write-ColorOutput Yellow "🌐 To port-forward for local access:"
    Write-ColorOutput White "kubectl port-forward -n $Namespace deployment/signal-engine-dev-signal-engine-helm 8080:8080"
}

Write-ColorOutput Green "🎉 SignalEngine build and deployment completed!"
Write-Output ""
Write-ColorOutput Yellow "Next steps:"
Write-ColorOutput White "1. Monitor pod status: kubectl get pods -n $Namespace"
Write-ColorOutput White "2. Check application logs: kubectl logs -n $Namespace -l app.kubernetes.io/name=signal-engine-helm"
Write-ColorOutput White "3. Test API endpoint: kubectl port-forward -n $Namespace svc/signal-engine-dev-signal-engine-helm 8080:8080"
Write-ColorOutput White "4. View Kubernetes dashboard or use kubectl describe for more details"
