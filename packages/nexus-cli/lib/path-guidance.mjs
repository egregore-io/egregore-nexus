import { delimiter, join } from "node:path";

export function pathGuidance({ platform, shell, pathValue = "", prefix, home }) {
  if (!prefix) return null;

  const binDir = platform === "win32" ? prefix : join(prefix, "bin");
  if (pathContains(pathValue, binDir, platform)) return null;

  const heading = [
    "Nexus installed successfully, but its command directory is not on PATH:",
    `  ${binDir}`,
    "",
  ];

  if (platform === "win32") {
    const quoted = powershellQuote(binDir);
    return [...heading,
      "Add it to your Windows user PATH in PowerShell:",
      `  $nexusBin = '${quoted}'; $userPath = [Environment]::GetEnvironmentVariable('Path', 'User'); [Environment]::SetEnvironmentVariable('Path', (($userPath + ';' + $nexusBin).Trim(';')), 'User')`,
      "Then open a new terminal and run `nexus --version`.",
    ].join("\n");
  }

  const shellName = shell?.split("/").pop();
  const quotedBin = shellQuote(binDir);
  if (shellName === "fish") {
    return [...heading,
      "Add it to Fish's universal PATH:",
      `  fish_add_path --universal ${quotedBin}`,
      "Then run `nexus --version`.",
    ].join("\n");
  }

  const profile = shellName === "zsh" ? ".zshrc" : shellName === "bash" ? ".bashrc" : ".profile";
  const homeLabel = home ? "$HOME" : "~";
  return [...heading,
    `Add it to ${profile}:`,
    `  printf '\\nexport PATH="%s:$PATH"\\n' ${quotedBin} >> "${homeLabel}/${profile}" && source "${homeLabel}/${profile}"`,
    "Then run `nexus --version`.",
  ].join("\n");
}

function pathContains(pathValue, expected, platform) {
  const separator = platform === "win32" ? ";" : delimiter;
  const normalize = platform === "win32"
    ? (value) => trimSeparators(value).toLowerCase()
    : trimSeparators;
  const target = normalize(expected);
  return pathValue.split(separator).some((entry) => normalize(entry) === target);
}

function trimSeparators(value) {
  return value.trim().replace(/[\\/]+$/, "");
}

function shellQuote(value) {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function powershellQuote(value) {
  return value.replaceAll("'", "''");
}
