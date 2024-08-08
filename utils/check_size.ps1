param (
    [string]$Path
)

if (Test-Path $Path -PathType Leaf) {
    # Nếu là file
    $file = Get-Item $Path
    $size = $file.Length
    
    Write-Output "Size of file $Path : $size bytes"
}

elseif (Test-Path $Path -PathType Container) {
    # Nếu là thư mục
    $size = Get-ChildItem $Path -Recurse | Measure-Object -Sum Length | Select-Object -ExpandProperty Sum
    Write-Output "Size of folder $Path : $size bytes"
}

else {
    Write-Output "Not found $Path"
}