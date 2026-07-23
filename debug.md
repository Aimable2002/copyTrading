USED THIS STEPS TO DISABLE MT5 AUTO-UPDATE AND DOWNLOADED COMES WITH RESTAERT MODEL MODE 

PROBLEM
MT5 comes with webinstaller folder hidden this one force the live updates and auto update,
when this auto updates triggered, the provision script can not work as it hangs waiting for human intervation click to restarts the app which breaks the whole point of the automation and therefore fail and crash

THE CMDS USED 

# 1. Take ownership of the directory
takeown /F "$env:APPDATA\MetaQuotes\WebInstall" /A /R /D Y

# 2. Grant Full Control to Administrators
icacls "$env:APPDATA\MetaQuotes\WebInstall" /grant Administrators:F /T /C /L /Q

# 3. Strip any hidden/system/read-only attributes
attrib -h -r -s "$env:APPDATA\MetaQuotes\WebInstall\*.*" /s /d

# 4. Delete the folder
Remove-Item "$env:APPDATA\MetaQuotes\WebInstall" -Recurse -Force

# Testing cmd to see if file is gone
Test-Path "$env:APPDATA\MetaQuotes\WebInstall"



THE RESULTS OF RUNNING THE ABOVE CMDS

Windows PowerShell
Copyright (C) Microsoft Corporation. All rights reserved.

Try the new cross-platform PowerShell https://aka.ms/pscore6

PS C:\Windows\system32> takeown /F "$env:APPDATA\MetaQuotes\WebInstall" /A /R /D Y

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt4clw.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt4clwdata.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clw64.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clwavx264.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clwdata.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clwide64.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clwideavx264.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clwtst64.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5clwtstavx264.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5onnxavx2.png" now owned by the administrators group.

SUCCESS: The file (or folder): "C:\Users\ISO\AppData\Roaming\MetaQuotes\WebInstall\mt5onnxavx264.png" now owned by the administrators group.
PS C:\Windows\system32> icacls "$env:APPDATA\MetaQuotes\WebInstall" /grant Administrators:F /T /C /L /Q
Successfully processed 12 files; Failed processing 0 files
PS C:\Windows\system32> attrib -h -r -s "$env:APPDATA\MetaQuotes\WebInstall\*.*" /s /d
PS C:\Windows\system32> Remove-Item "$env:APPDATA\MetaQuotes\WebInstall" -Recurse -Force
PS C:\Windows\system32> Test-Path "$env:APPDATA\MetaQuotes\WebInstall"
False
PS C:\Windows\system32>


