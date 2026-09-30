import { execFile } from "node:child_process";
import { win32 } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

/** Restrict an exclusively created temporary file before its sensitive contents are written. */
export async function makeWindowsFilePrivate(path) {
  if (process.platform !== "win32") throw new Error("Windows private-file helper used on another platform");
  const script = String.raw`
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$path=([Console]::In.ReadToEnd() | ConvertFrom-Json).path
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
$acl=New-Object Security.AccessControl.FileSecurity
$acl.SetOwner($sid)
$acl.SetAccessRuleProtection($true,$false)
$acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule($sid,'FullControl','Allow')))
Set-Acl -LiteralPath $path -AclObject $acl
$actual=Get-Acl -LiteralPath $path
if($actual.GetOwner([Security.Principal.SecurityIdentifier]).Value -ne $sid.Value -or -not $actual.AreAccessRulesProtected) {
  throw 'private file owner/DACL verification failed'
}
foreach($rule in $actual.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
  if($rule.IdentityReference.Value -ne $sid.Value) { throw 'private file retained foreign access' }
}
`;
  const root = process.env.SystemRoot ?? process.env.SYSTEMROOT ?? "C:\\Windows";
  if (!/^[A-Za-z]:\\/.test(root)) throw new Error("invalid Windows system directory");
  const pending = execFileAsync(win32.join(root, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
    ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
    { timeout: 15_000, maxBuffer: 65536, windowsHide: true, env: { SystemRoot: root } });
  pending.child.stdin.end(JSON.stringify({ path }));
  await pending;
}
