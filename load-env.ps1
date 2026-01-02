param(
    [string]$EnvFile = "dev.env"
)

Get-Content $EnvFile | ForEach-Object {
    if ($_ -match '^\s*#' -or -not $_) { return }

    $key, $value = $_ -split '=', 2
    $value = $value.Trim('"')
    Set-Item -Path Env:$key -Value $value
}
