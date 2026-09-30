export function platformTarget({ platform, arch, glibcVersionRuntime }) {
  if (platform === "linux" && glibcVersionRuntime) {
    if (arch === "x64") return "linux-x64-gnu";
    if (arch === "arm64") return "linux-arm64-gnu";
  }

  if (platform === "darwin") {
    if (arch === "x64") return "darwin-x64";
    if (arch === "arm64") return "darwin-arm64";
  }

  if (platform === "win32" && arch === "x64") {
    return "win32-x64-msvc";
  }

  return undefined;
}
