<#
.SYNOPSIS
    Télécharge le jeu de données Sparkov depuis Kaggle dans data/raw/.

.DESCRIPTION
    Rend l'acquisition des données reproductible : n'importe qui clonant le
    dépôt retrouve exactement le même jeu de données par une seule commande.
    C'est un critère de reproductibilité attendu dans un projet Data Engineering.

.NOTES
    Prérequis :
      1. pip install kaggle
      2. Jeton API Kaggle placé dans %USERPROFILE%\.kaggle\kaggle.json
         (Kaggle -> Settings -> API -> Create New Token)
#>

$ErrorActionPreference = "Stop"

$Dataset = "kartik2112/fraud-detection"
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Destination = Join-Path $RepoRoot "data\raw"

# --- Vérification des prérequis ------------------------------------------
$CredentialPath = Join-Path $env:USERPROFILE ".kaggle\kaggle.json"
if (-not (Test-Path $CredentialPath)) {
    throw "Jeton Kaggle introuvable : $CredentialPath. Voir data/README.md."
}

if ($null -eq (Get-Command kaggle -ErrorAction SilentlyContinue)) {
    throw "CLI Kaggle introuvable. Lancer : pip install kaggle"
}

# --- Téléchargement -------------------------------------------------------
if (-not (Test-Path $Destination)) {
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
}

Write-Host "Telechargement de $Dataset vers $Destination ..."
kaggle datasets download -d $Dataset -p $Destination --unzip

# --- Contrôle du résultat -------------------------------------------------
$Expected = @("fraudTrain.csv", "fraudTest.csv")
foreach ($File in $Expected) {
    $Path = Join-Path $Destination $File
    if (-not (Test-Path $Path)) {
        throw "Fichier attendu manquant apres telechargement : $File"
    }
    $SizeMb = [math]::Round((Get-Item $Path).Length / 1MB, 1)
    Write-Host ("  OK  {0,-16} {1,8} Mo" -f $File, $SizeMb)
}

Write-Host "Termine."
