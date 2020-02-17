function Get-Config {
    param(
        [string]$ConfigFilePath
    )

    if(Test-Path $ConfigFilePath) {
        return Get-Content $ConfigFilePath | ConvertFrom-Json
    }
    else {
        return $null
    }
}

function Get-NextId {
    param(
        [string]$ProcessDir
    )

    if(!$ProcessDir.EndsWith("\")) {
        $ProcessDir += "\"
    }

    $NextIdPath = $ProcessDir + "state\nextid.txt"

    if(Test-Path $NextIdPath) {
        $nextIdContent = Get-Content $NextIdPath
        if($null -ne $nextIdContent) {
            return $nextIdContent
        }
        else {
            return -1
        }
    }
    else {
        return -1
    }
}

function Get-AuthorizationHeader {
    param(
        [string]$ApiId,
        [string]$ApiKey
    )

    $authString = [System.Text.Encoding]::UTF8.GetBytes("$($ApiId):$($ApiKey)")
    $convertedAuthString = [System.Convert]::ToBase64String($authString)
    $headers = @{ "Authorization"="Basic " + $convertedAuthString }

    return $headers
}

function Invoke-ImpervaLogRequest {
    param(
        [string]$BaseUri,
        [string]$Id,
        [string]$ApiId,
        [string]$ApiKey
    )

    $headers = Get-AuthorizationHeader -ApiId $ApiId -ApiKey $ApiKey
    $uri = "$BaseUri" + "$Id"

    try {
        $req = Invoke-WebRequest -Uri $uri -Headers $headers
    } catch [System.Net.WebException] 
    {
        $statusCode = [int]$_.Exception.Response.StatusCode
        # $html = $_.Exception.Response.StatusDescription

        $req = [PSCustomObject] @{
            StatusCode = $statusCode
        }
    }

    return $req

    # if($req.StatusCode -eq 200) {
    #     return $req.Content
    # }
    # else {
    #     return $null
    # }
}

function Write-ImpervaLog {
    param(
        [byte[]]$LogContent,
        [string]$OutputPath
    )

    $writeFailed = $false

    # Find "|==|`n" separator
    $separator = [System.Text.Encoding]::UTF8.GetBytes("|==|`n")
    $index = $LogContent.IndexOf($separator[0])
    Write-Debug "Separator found at index $index."

    # Separate header from compressed log content (not doing anything with this yet)
    # $headerContent = [System.Text.Encoding]::UTF8.GetString($LogContent[0..($index - 1)])

    # Separate compressed content from header, separator and 2 extra bytes (for some reason)
    [byte[]]$compressedContent = $LogContent[($index + 7)..($LogContent.Length - 1)]
    Write-Debug "Compressed content length is $($compressContent.Length)."

    try {
        # Open compressed log content from memory
        Write-Debug "Creating MemoryStream with compressed content."
        $input = [System.IO.MemoryStream]::new($compressedContent)
        Write-Debug "MemoryStream created."

        # Open FileStream for writing file to disk
        $output = New-Object System.IO.FileStream $OutputPath, ([IO.FileMode]::Create), ([IO.FileAccess]::Write), ([IO.FileShare]::None)

        # Take compressed content from memory, decompress and pass to FileStream for writing to disk
        $defStream = [System.IO.Compression.DeflateStream]::new($input, [IO.Compression.CompressionMode]::Decompress)
        Write-Debug "Decompressing content and writing to file stream."
        $defStream.CopyTo($output)
        Write-Debug "Content decompressed and written to file stream (output path: $OutputPath)."
    } catch {
        Write-Error "An error occurred: $_"
        $writeFailed = $true
    } finally {
        # Close all streams
        Write-Debug "Closing all streams."
        $output.Close()
        $defStream.Close()
        $input.Close()
    }

    return !$writeFailed
}

function Set-NextID {
    param(
        [switch]$Increment,
        [string]$Id,
        [string]$ProcessDir
    )

    if($Increment) {
        $s = $Id.Split('_')
        $s2 = $s[1].Split('.')
        [int]$indexNo = $s2[0]
        $indexNo++

        $newId = $s[0] + '_' + $indexNo + '.' + $s2[1]
    }
    else {
        $newId = $Id
    }

    if(!$ProcessDir.EndsWith("\")) {
        $ProcessDir += "\"
    }

    $NextIdPath = $ProcessDir + "state\nextid.txt"

    $newId | Set-Content -Path $NextIdPath
}

function Get-NextIdFromIndex {
    $uri = $script:Config.base_url + "logs.index"

    $req = Invoke-WebRequest -Uri $uri -Headers (Get-AuthorizationHeader -ApiId $script:Config.api_id -ApiKey $script:Config.api_key)
    # Convert Bytes to UTF-8 String and strip unnecessary last byte
    $indexContent = [System.Text.Encoding]::UTF8.GetString($req.Content[0..($req.Content.Length - 2)])
    # Split by newline to get first available log.
    $firstAvailable = $indexContent.Split("`n")[0]

    return $firstAvailable
}

# Script starts
# Reads config
# Reads next ID
# Attempts to download next log via ID
# Saves log locally
# Sets next ID

Write-Host "Loading config file $($args[0])."
$script:Config = Get-Config $args[0]

# TODO: Validate config

if($null -eq $script:Config) {
    Write-Error "Unable to read $ConfigFilePath - check if file exists and permissions are correct."
    exit 1
}

$retryCount = 0
$404retryCount = 0

# Main script loop
while($true) {
    $nextId = Get-NextId $script:Config.process_dir

    if($nextId -ne -1) {
        Write-Host "Attempting to download log $nextId..."
        $request = Invoke-ImpervaLogRequest -BaseUri $script:Config.base_url -id $nextId -ApiId $script:Config.api_id -ApiKey $script:Config.api_key

        if($request.StatusCode -eq 200) {
            $404retryCount = 0
            [byte[]]$content = $request.Content

            if($null -ne $content) {
                $outputPath = $script:Config.process_dir + "\$nextId"
                $writeSuccess = Write-ImpervaLog -LogContent $content -OutputPath ($script:Config.process_dir + "\$nextId")

                if($writeSuccess) {
                    Write-Host "Successfully wrote log to `"$outputPath`"."
                    $retryCount = 0
                    Set-NextId -Increment -Id $nextId -ProcessDir $script:Config.process_dir
                    
                    Write-Host "Sleeping for $($script:Config.sleep_time_success) seconds..."
                    Start-Sleep -Seconds $script:Config.sleep_time_success
                }
                else {
                    if($retryCount -lt $script:Config.max_retry) {
                        $retryCount++
                        Write-Warning "Failed to write file $outputPath. This is try number $retryCount. Sleeping for $($script:Config.sleep_time_error) seconds before trying again..."
                        Start-Sleep -Seconds $script:Config.sleep_time_error
                    }
                    else {
                        Write-Error "Failed to write file $outputPath. Maximum number of retries ($($script:Config.max_retry)) has been reached. Script will now exit."
                        exit 2
                    }
                }
            }
            else {
                
            }        
        }
        elseif($request.StatusCode -eq 404) {
            if($404retryCount -lt $script:Config.max_retry) {
                $404retryCount++
                Write-Host "Received 404 response for log $nextId. This is try $404retryCount out of $($script:Config.max_retry). Log probably doesn't exist yet. Sleeping for $($script:Config.sleep_time_error) seconds before trying again."
                Start-Sleep -Seconds $script:Config.sleep_time_error
            }
            else {
                $nextId = -1
            }
        }
    }
    else {
        Write-Host "Retrieving first available log from the index..."
        $nextId = Get-NextIdFromIndex
        Write-Host "First available log is $nextId."
        Set-NextId -Id $nextId -ProcessDir $script:Config.process_dir
    }
}