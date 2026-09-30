import { execFile } from "node:child_process";
import { win32 } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

/** Only disposable test paths; production never changes an operator's ACL. */
export async function windowsFixtureAcl(path: string, mode: "secure-tree" | "secure" | "writable" | "readable" | "wrong-owner") {
  if (process.platform !== "win32") throw new Error("Windows ACL fixture used on another platform");
  const script = String.raw`
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$request=[Console]::In.ReadToEnd() | ConvertFrom-Json
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
$items=@(Get-Item -LiteralPath $request.path -Force)
if($request.mode -eq 'secure-tree') { $items += @(Get-ChildItem -LiteralPath $request.path -Recurse -Force) }
foreach($item in $items) {
  $acl=Get-Acl -LiteralPath $item.FullName
  if($request.mode -eq 'secure' -or $request.mode -eq 'secure-tree') {
    $acl.SetOwner($sid)
    $acl.SetAccessRuleProtection($true,$false)
    foreach($rule in @($acl.Access)) { [void]$acl.RemoveAccessRuleSpecific($rule) }
    if($item.PSIsContainer) {
      $rule=New-Object Security.AccessControl.FileSystemAccessRule($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow')
    } else { $rule=New-Object Security.AccessControl.FileSystemAccessRule($sid,'FullControl','Allow') }
    $acl.AddAccessRule($rule)
  } elseif($request.mode -eq 'wrong-owner') {
    $acl.SetOwner((New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')))
  } else {
    $rights=if($request.mode -eq 'writable'){'Write'}else{'ReadAndExecute'}
    $acl.AddAccessRule((New-Object Security.AccessControl.FileSystemAccessRule((New-Object Security.Principal.SecurityIdentifier('S-1-1-0')),$rights,'Allow')))
  }
  Set-Acl -LiteralPath $item.FullName -AclObject $acl
}
`;
  const pending = execFileAsync(win32.join(process.env.SystemRoot ?? "C:\\Windows",
    "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
  ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
  { timeout: 15_000, maxBuffer: 1024 * 1024, windowsHide: true,
    // Do not inherit PowerShell 7's PSModulePath into the Windows PowerShell 5 verifier.
    env: { SystemRoot: process.env.SystemRoot ?? "C:\\Windows" } });
  pending.child.stdin!.end(JSON.stringify({ path, mode }));
  await pending;
}
