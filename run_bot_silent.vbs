Set WshShell = CreateObject("WScript.Shell")
proj = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
cmd = "cmd /c cd /d """ & proj & """ && run_bot.bat"
WshShell.Run cmd, 0, False