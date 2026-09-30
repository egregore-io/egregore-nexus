// Static program only. Paths arrive as JSON on stdin, never PowerShell/C# source interpolation.
// Every security descriptor, file ID and content hash is read from the same no-follow handle.
export const WINDOWS_AUTHORITY_SCRIPT = String.raw`
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

public static class NexusTransportAuthority {
  [StructLayout(LayoutKind.Sequential)]
  public struct Info {
    public uint Attributes;
    public System.Runtime.InteropServices.ComTypes.FILETIME Created, Accessed, Written;
    public uint Volume, SizeHigh, SizeLow, Links, IndexHigh, IndexLow;
  }
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
  static extern SafeFileHandle CreateFileW(string path, uint access, uint share, IntPtr security,
    uint disposition, uint flags, IntPtr template);
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool GetFileInformationByHandle(SafeFileHandle handle, out Info info);
  [DllImport("advapi32.dll", SetLastError=true)]
  static extern uint GetSecurityInfo(SafeFileHandle handle, uint type, uint information,
    out IntPtr owner, out IntPtr group, out IntPtr dacl, out IntPtr sacl, out IntPtr descriptor);
  [DllImport("advapi32.dll")]
  static extern uint GetSecurityDescriptorLength(IntPtr descriptor);
  [DllImport("kernel32.dll")]
  static extern IntPtr LocalFree(IntPtr memory);

  public class Ace { public int type, flags; public string sid; public uint mask; }
  public class Security {
    public string owner, digest;
    public bool daclPresent;
    public List<Ace> aces = new List<Ace>();
  }
  public class Capture {
    public string path, identity, sha256, bytes;
    public bool directory;
    public long size;
    public Security security;
  }
  static string Hex(byte[] bytes) { return BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant(); }
  static string Identity(Info info) {
    return info.Volume.ToString("x8") + ":" + info.IndexHigh.ToString("x8") + info.IndexLow.ToString("x8");
  }
  static Info ReadInfo(SafeFileHandle handle) {
    Info info;
    if (!GetFileInformationByHandle(handle, out info)) throw new Win32Exception(Marshal.GetLastWin32Error());
    if ((info.Attributes & 0x400) != 0) throw new IOException("no-follow refused reparse point");
    return info;
  }
  static Security ReadSecurity(SafeFileHandle handle) {
    IntPtr owner, group, dacl, sacl, pointer;
    uint code = GetSecurityInfo(handle, 1, 5, out owner, out group, out dacl, out sacl, out pointer);
    if (code != 0) throw new Win32Exception((int)code);
    try {
      uint size = GetSecurityDescriptorLength(pointer);
      if (size == 0 || size > 1048576) throw new IOException("invalid security descriptor length");
      byte[] data = new byte[size]; Marshal.Copy(pointer, data, 0, data.Length);
      var descriptor = new RawSecurityDescriptor(data, 0);
      var result = new Security();
      result.owner = descriptor.Owner == null ? null : descriptor.Owner.Value;
      result.daclPresent = (descriptor.ControlFlags & ControlFlags.DiscretionaryAclPresent) != 0
        && descriptor.DiscretionaryAcl != null;
      using (var hash = SHA256.Create()) result.digest = Hex(hash.ComputeHash(data));
      if (result.daclPresent) foreach (GenericAce generic in descriptor.DiscretionaryAcl) {
        var common = generic as CommonAce;
        result.aces.Add(new Ace { type=(int)generic.AceType, flags=(int)generic.AceFlags,
          sid=common == null ? null : common.SecurityIdentifier.Value,
          mask=common == null ? 0 : unchecked((uint)common.AccessMask) });
      }
      return result;
    } finally { LocalFree(pointer); }
  }
  static SafeFileHandle Open(string path, bool directory) {
    // READ_CONTROL plus file read data. Share flags permit normal lifecycle changes, which are
    // detected by the subsequent descriptor/identity comparisons rather than silently trusted.
    var handle = CreateFileW(path, 0x20000u | (directory ? 0u : 0x80000000u), 7, IntPtr.Zero,
      3, 0x00200000u | 0x02000000u, IntPtr.Zero);
    if (handle.IsInvalid) { int code=Marshal.GetLastWin32Error(); handle.Dispose(); throw new Win32Exception(code); }
    return handle;
  }
  public static Capture Read(string path, bool directory, bool includeBytes) {
    using (var handle = Open(path, directory)) {
      Info before = ReadInfo(handle);
      if (((before.Attributes & 16) != 0) != directory) throw new IOException("wrong authority object type");
      var security = ReadSecurity(handle);
      var result = new Capture { path=path, directory=directory, identity=Identity(before), security=security };
      if (!directory) {
        ulong length = ((ulong)before.SizeHigh << 32) | before.SizeLow;
        if (length > (includeBytes ? 65536ul : 134217728ul)) throw new IOException("transport authority file too large");
        using (var stream = new FileStream(handle, FileAccess.Read)) {
          result.size = stream.Length;
          using (var hash = SHA256.Create()) {
            if (includeBytes) {
              using (var memory = new MemoryStream()) {
                stream.CopyTo(memory);
                if (memory.Length > 65536) throw new IOException("manifest grew beyond limit");
                byte[] data=memory.ToArray(); result.bytes=Convert.ToBase64String(data);
                result.sha256=Hex(hash.ComputeHash(data));
              }
            } else result.sha256=Hex(hash.ComputeHash(stream));
          }
          Info after=ReadInfo(handle);
          if (Identity(after) != result.identity || before.SizeHigh != after.SizeHigh || before.SizeLow != after.SizeLow
            || before.Written.dwHighDateTime != after.Written.dwHighDateTime || before.Written.dwLowDateTime != after.Written.dwLowDateTime
            || ReadSecurity(handle).digest != security.digest) throw new IOException("authority changed during read");
        }
      }
      using (var current = Open(path, directory)) {
        if (Identity(ReadInfo(current)) != result.identity || ReadSecurity(current).digest != security.digest)
          throw new IOException("authority identity or DACL changed during capture");
      }
      return result;
    }
  }
}
'@
$request = [Console]::In.ReadToEnd() | ConvertFrom-Json
$items = @()
foreach ($item in $request) {
  $items += [NexusTransportAuthority]::Read([string]$item.path, [bool]$item.directory, [bool]$item.includeBytes)
}
@{ user = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value; items = @($items) } |
  ConvertTo-Json -Depth 12 -Compress
`;
