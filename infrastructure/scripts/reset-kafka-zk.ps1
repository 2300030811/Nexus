[CmdletBinding()]
param(
    [switch]$Full
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeDir = Resolve-Path (Join-Path $scriptDir "..")

Push-Location $composeDir
try {
    Write-Host "[1/4] Stopping Kafka and ZooKeeper ..."
    docker compose stop kafka zookeeper | Out-Null

    Write-Host "[2/4] Removing Kafka and ZooKeeper containers ..."
    docker compose rm -f kafka zookeeper | Out-Null

    if ($Full) {
        Write-Host "[3/4] Recreating Kafka and ZooKeeper ..."
        docker compose up -d --force-recreate zookeeper kafka | Out-Null
    }
    else {
        Write-Host "[3/4] Starting ZooKeeper and Kafka ..."
        docker compose up -d zookeeper kafka | Out-Null
    }

    Write-Host "[4/4] Waiting for Kafka to become healthy ..."
    $maxAttempts = 30
    $sleepSeconds = 2

    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        $health = docker inspect --format "{{.State.Health.Status}}" nexus-kafka 2>$null
        if ($health -eq "healthy") {
            Write-Host "Kafka is healthy."
            docker compose ps kafka
            exit 0
        }

        Start-Sleep -Seconds $sleepSeconds
    }

    Write-Error "Kafka did not become healthy in time. Check logs with: docker compose logs --tail 120 kafka"
    exit 1
}
finally {
    Pop-Location
}
