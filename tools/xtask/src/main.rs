#![forbid(unsafe_code)]

use std::{
    env, fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("bundle") => bundle(arguments.collect()),
        Some(command) => Err(format!("unknown command `{command}`")),
        None => Err("expected a command; available command: bundle".to_owned()),
    }
}

fn bundle(arguments: Vec<String>) -> Result<(), String> {
    let options = BundleOptions::parse(arguments)?;
    let workspace = workspace_root();
    let target = match options.target {
        Some(target) => target,
        None => rust_host(&workspace)?,
    };

    run_command(Command::new("npm").arg("ci").current_dir(workspace.join("web")), "install Web dependencies")?;
    run_command(Command::new("npm").args(["run", "build"]).current_dir(workspace.join("web")), "build Web assets")?;

    let mut cargo = Command::new("cargo");
    cargo
        .args(["build", "--locked", "--package", "kgi", "--profile"])
        .arg(&options.profile)
        .arg("--message-format=json-render-diagnostics")
        .current_dir(&workspace);
    if options.explicit_target {
        cargo.args(["--target", &target]);
    }
    let binary_source = cargo_binary_output(&mut cargo)?;

    let version = env!("CARGO_PKG_VERSION");
    let final_directory = workspace.join("dist").join(format!("kgi-{version}-{target}"));
    let temporary_directory = workspace.join("dist").join(format!(".kgi-{version}-{target}.tmp-{}", std::process::id()));
    if temporary_directory.exists() {
        fs::remove_dir_all(&temporary_directory).map_err(|error| format!("remove stale bundle directory: {error}"))?;
    }

    let result = construct_bundle(&workspace, &temporary_directory, &target, &binary_source);
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&temporary_directory);
        return Err(error);
    }

    if final_directory.exists() {
        fs::remove_dir_all(&final_directory).map_err(|error| format!("remove previous complete bundle: {error}"))?;
    }
    fs::rename(&temporary_directory, &final_directory).map_err(|error| format!("publish bundle atomically: {error}"))?;
    println!("created {}", final_directory.display());
    Ok(())
}

fn construct_bundle(workspace: &Path, temporary_directory: &Path, target: &str, binary_source: &Path) -> Result<(), String> {
    let binary_directory = temporary_directory.join("bin");
    let web_directory = temporary_directory.join("share/kgi/web");
    fs::create_dir_all(&binary_directory).map_err(|error| format!("create bundle binary directory: {error}"))?;
    copy_directory(&workspace.join("web/dist"), &web_directory).map_err(|error| format!("copy Web build: {error}"))?;

    let binary_name = binary_source
        .file_name()
        .ok_or_else(|| format!("Cargo reported an executable without a filename: {}", binary_source.display()))?;
    fs::copy(binary_source, binary_directory.join(binary_name))
        .map_err(|error| format!("copy {}: {error}", binary_source.display()))?;
    fs::copy(workspace.join("LICENSE"), temporary_directory.join("LICENSE")).map_err(|error| format!("copy license: {error}"))?;

    let source_commit = command_output(Command::new("git").args(["rev-parse", "HEAD"]).current_dir(workspace), "read source commit")?;
    let release = format!(
        "{{\n  \"version\": \"{}\",\n  \"source_commit\": \"{}\",\n  \"target\": \"{}\"\n}}\n",
        env!("CARGO_PKG_VERSION"),
        source_commit.trim(),
        target
    );
    fs::write(temporary_directory.join("release.json"), release).map_err(|error| format!("write release provenance: {error}"))?;
    Ok(())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().and_then(Path::parent).expect("xtask must remain under tools/xtask").to_owned()
}

fn rust_host(workspace: &Path) -> Result<String, String> {
    let output = command_output(Command::new("rustc").arg("-vV").current_dir(workspace), "read Rust host target")?;
    output
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(str::to_owned)
        .ok_or_else(|| "rustc did not report a host target".to_owned())
}

fn run_command(command: &mut Command, purpose: &str) -> Result<(), String> {
    let status = command.status().map_err(|error| format!("{purpose}: {error}"))?;
    if status.success() { Ok(()) } else { Err(format!("{purpose}: command exited with {status}")) }
}

fn cargo_binary_output(command: &mut Command) -> Result<PathBuf, String> {
    let output = command.output().map_err(|error| format!("build the KGI binary: {error}"))?;
    let stdout = String::from_utf8(output.stdout).map_err(|error| format!("parse Cargo build output: {error}"))?;
    let executable = kgi_executable_from_cargo_messages(&stdout)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("build the KGI binary: command exited with {}\n{}", output.status, stderr.trim()));
    }

    executable.ok_or_else(|| "Cargo completed without reporting the KGI binary artifact".to_owned())
}

fn kgi_executable_from_cargo_messages(messages: &str) -> Result<Option<PathBuf>, String> {
    let mut executable = None;
    for line in messages.lines().filter(|line| !line.is_empty()) {
        let message: serde_json::Value = serde_json::from_str(line).map_err(|error| format!("parse Cargo build message: {error}"))?;

        if message["reason"] == "compiler-message" {
            if let Some(rendered) = message["message"]["rendered"].as_str() {
                eprint!("{rendered}");
            }
            continue;
        }

        let is_kgi_binary = message["reason"] == "compiler-artifact"
            && message["target"]["name"] == "kgi"
            && message["target"]["kind"].as_array().is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"));
        if is_kgi_binary {
            let path = message["executable"]
                .as_str()
                .ok_or_else(|| "Cargo reported the KGI binary artifact without an executable path".to_owned())?;
            executable = Some(PathBuf::from(path));
        }
    }
    Ok(executable)
}

fn command_output(command: &mut Command, purpose: &str) -> Result<String, String> {
    let output = command.output().map_err(|error| format!("{purpose}: {error}"))?;
    if !output.status.success() {
        return Err(format!("{purpose}: command exited with {}", output.status));
    }
    String::from_utf8(output.stdout).map_err(|error| format!("{purpose}: {error}"))
}

fn copy_directory(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&source_path, &destination_path)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

struct BundleOptions {
    target: Option<String>,
    explicit_target: bool,
    profile: String,
}

impl BundleOptions {
    fn parse(arguments: Vec<String>) -> Result<Self, String> {
        let mut target = None;
        let mut profile = "release".to_owned();
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--target" => {
                    target = Some(next_value(&mut arguments, "--target")?);
                }
                "--profile" => {
                    profile = next_value(&mut arguments, "--profile")?;
                }
                _ => return Err(format!("unknown bundle argument `{argument}`")),
            }
        }
        Ok(Self { explicit_target: target.is_some(), target, profile })
    }
}

fn next_value(arguments: &mut impl Iterator<Item = String>, option: &str) -> Result<String, String> {
    arguments.next().filter(|value| !value.is_empty()).ok_or_else(|| format!("{option} requires a value"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::kgi_executable_from_cargo_messages;

    #[test]
    fn uses_reported_test_profile_executable() {
        let messages = concat!(
            r#"{"reason":"compiler-artifact","target":{"name":"dependency","kind":["lib"]},"executable":null}"#,
            "\n",
            r#"{"reason":"compiler-artifact","target":{"name":"kgi","kind":["bin"]},"executable":"/workspace/target/debug/kgi"}"#,
        );

        assert_eq!(kgi_executable_from_cargo_messages(messages), Ok(Some(PathBuf::from("/workspace/target/debug/kgi"))));
    }

    #[test]
    fn preserves_target_executable_suffix() {
        let messages = r#"{"reason":"compiler-artifact","target":{"name":"kgi","kind":["bin"]},"executable":"/workspace/target/x86_64-pc-windows-gnu/release/kgi.exe"}"#;

        assert_eq!(
            kgi_executable_from_cargo_messages(messages),
            Ok(Some(PathBuf::from("/workspace/target/x86_64-pc-windows-gnu/release/kgi.exe")))
        );
    }
}
