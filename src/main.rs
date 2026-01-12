use std::collections::BTreeMap as Map;
use std::collections::BTreeSet as Set;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, ExitStatus, Stdio};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use thiserror::Error;

#[cfg(windows)]
const KPSE_SEPARATOR: char = ';';
#[cfg(unix)]
const KPSE_SEPARATOR: char = ':';

#[cfg(windows)]
const PATH_ENV_SEPARATOR: &str = ";";
#[cfg(unix)]
const PATH_ENV_SEPARATOR: &str = ":";

const CONFIG_FILE_NAME: &str = "dtmgr.toml";

#[derive(Parser)]
#[command(version, about, long_about = None, arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Install {},

    #[command(disable_help_flag = true, disable_version_flag = true)]
    Run {
        #[arg(allow_hyphen_values = true)]
        program: String,

        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        args: Vec<String>,
    }
}

#[derive(Error, Debug)]
pub enum DtMgrError {
    #[error("unable to parse configuration file `dtmgr.toml`")]
    ParseConfig {
        #[source] source: toml::de::Error
    },
    #[error("unable to read file ({path})")]
    ReadFile {
        path: PathBuf,
        #[source] source: std::io::Error
    },
    #[error("unable to hash config")]
    HashConfig {
        #[source] source: postcard::Error
    },
    #[error("system failure executing a command")]
    CommandExecution {
        #[source] source: std::io::Error
    },
    #[error("command `{command}` exited with non-zero exit code ({code:?})")]
    CommandStatus {
        command: String,
        code: Option<i32>,
    },
    #[error("failed to parse json")]
    JsonParse {
        #[source] source: serde_json::Error
    },
    #[error("failed to retrieve current directory")]
    CurrentDirectory {
        #[source] source: std::io::Error
    },
    #[error("unable to find dtmgr.toml in current directory ({cwd}) or any of its parents")]
    FindConfig {
        cwd: PathBuf
    },
    #[error("unable to create directory ({dir})")]
    CreateDirectory {
        dir: PathBuf,
        #[source] source: std::io::Error,
    },
    #[error("unable to write to file ({file})")]
    WriteFile {
        file: PathBuf,
        #[source] source: std::io::Error,
    },
    #[error("unable to create symlink (src: {src}, dst: {dst})")]
    CreateSymlink {
        src: PathBuf,
        dst: PathBuf,
        #[source] source: std::io::Error,
    },
    #[error("unable to remove directory ({dir})")]
    RemoveDirectory {
        dir: PathBuf,
        #[source] source: std::io::Error,
    }
}

#[derive(Debug, Deserialize, Serialize, Hash)]
pub struct DtMgrConfig {
    dependencies: Set<String>
}

// https://svn.tug.org:8369/texlive/trunk/Master/tlpkg/doc/json-formats.txt?view=markup
#[derive(Debug, Deserialize, Clone)]
pub struct TlPObjInfo {
    name: String,
    shortdesc: Option<String>,
    longdesc: Option<String>,
    category: Option<String>,
    catalogue: Option<String>,
    containerchecksum: Option<String>,
    lrev: Option<u64>,
    rrev: Option<u64>,
    runsize: Option<u64>,
    docsize: Option<u64>,
    srcsize: Option<u64>,
    containersize: Option<u64>,
    srccontainersize: Option<u64>,
    doccontainersize: Option<u64>,
    available: bool,
    installed: Option<bool>,
    relocated: Option<bool>,
    runfiles: Option<Vec<String>>,
    srcfiles: Option<Vec<String>>,
    executes: Option<Vec<String>>,
    depends: Option<Vec<String>>,
    postactions: Option<Vec<String>>,
    docfiles: Option<Vec<TlPObjDocFile>>,
    binfiles: Option<Map<String, Vec<String>>>,
    binsize: Option<Map<String, u64>>,
    cataloguedata: Option<TlPObjInfoCatalogueData>,
    rcataloguedata: Option<TlPObjInfoCatalogueData>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlPObjDocFile {
    file: String,
    lang: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlPObjInfoCatalogueData {
    topics: Option<String>,
    version: Option<String>,
    license: Option<String>,
    ctan: Option<String>,
    date: Option<String>,
    related: Option<String>,
}

#[cfg(windows)]
fn cmd_crossplatform_static_args<I, S>(exe_and_args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr> {
    let mut cmd = Command::new("powershell");
    cmd.arg("-c");

    let mut command_string = String::new();
    command_string.push_str("& ");

    for (idx, elem) in exe_and_args.into_iter().enumerate() {
        let key = format!("DTMGR_ARG{}", idx);
        command_string.push_str("$Env:");
        command_string.push_str(key.as_str());
        command_string.push(' ');
        cmd.env(key, elem);
    }
    cmd.arg(command_string);
    cmd
}

#[cfg(unix)]
fn cmd_crossplatform_static_args<I, S>(exe_and_args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr> {
    let mut as_iter = exe_and_args.into_iter();
    let mut cmd = Command::new(as_iter.next().expect("exe_and_args should be nonempty"));
    cmd.args(as_iter);
    cmd
}

#[cfg(all(not(windows), not(unix)))]
fn cmd_crossplatform_static_args<I, S>(exe_and_args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr> {
    compile_error!("not sure how to spawn a command on this platform")
}

// TODO all of these .expect s should be replaced with proper tracing

fn get_texlive_root() -> Result<PathBuf, DtMgrError> {
    let kpse_out = cmd_crossplatform_static_args(["kpsewhich", "-var-value=TEXMFROOT"])
        .output().map_err(|e| DtMgrError::CommandExecution { source: e })?;

    if kpse_out.status.success() {
        Ok(PathBuf::from(String::from_utf8(kpse_out.stdout).expect("kpsewhich output is utf-8").trim()))
    } else {
        Err(DtMgrError::CommandStatus { command: "kpsewhich -var-value=TEXMFROOT".to_owned(), code: kpse_out.status.code() })
    }
}

fn get_texlive_platform() -> Result<String, DtMgrError> {
    let tlmgr_out = cmd_crossplatform_static_args(["tlmgr", "print-platform"])
        .output().map_err(|e| DtMgrError::CommandExecution { source: e })?;

    if tlmgr_out.status.success() {
        Ok(String::from_utf8(tlmgr_out.stdout).expect("tlmgr output is utf-8").trim().to_owned())
    } else {
        Err(DtMgrError::CommandStatus { command: "tlmgr print-platform".to_owned(), code: tlmgr_out.status.code() })
    }
}

fn install_packages_globally<'a, I, S>(packages: I) -> Result<(), DtMgrError>
where
    I: IntoIterator<Item = &'a S>,
    S: AsRef<str> + 'a {
    let mut packages_vec: Vec<&'a str> = Vec::new();
    for package in packages.into_iter() {
        packages_vec.push(package.as_ref());
    }

    let mut cmd = cmd_crossplatform_static_args(["tlmgr", "install"].into_iter().chain(packages_vec.iter().copied()));
    let out = cmd.status()
        .map_err(|e| DtMgrError::CommandExecution { source: e })?;

    if out.success() {
        Ok(())
    } else {
        Err(DtMgrError::CommandStatus { command: "tlmgr install ".to_owned() + packages_vec.join(" ").as_str(), code: out.code() })
    }
}

fn info_about_packages<'a, I, S>(packages: I) -> Result<Vec<TlPObjInfo>, DtMgrError>
where
    I: IntoIterator<Item = &'a S>,
    S: AsRef<str> + 'a {
    let mut packages_vec: Vec<&'a str> = Vec::new();
    for package in packages.into_iter() {
        packages_vec.push(package.as_ref());
    }

    let mut cmd = cmd_crossplatform_static_args(["tlmgr", "info", "--json"].into_iter().chain(packages_vec.iter().copied()));
    let out = cmd.output()
        .map_err(|e| DtMgrError::CommandExecution { source: e })?;
    if out.status.success() {
        let json = serde_json::from_slice::<Vec<TlPObjInfo>>(out.stdout.as_slice())
            .map_err(|e| DtMgrError::JsonParse { source: e })?;
        Ok(json)
    } else {
        Err(DtMgrError::CommandStatus { command: "tlmgr info --json ".to_owned() + packages_vec.join(" ").as_str(), code: out.status.code() })
    }
}

fn find_dtmgr_directory() -> Result<PathBuf, DtMgrError> {
    let initial: &Path = &*std::env::current_dir()
        .map_err(|e| DtMgrError::CurrentDirectory { source: e })?;
    let mut cwd: Option<&Path> = Some(initial);

    while let Some(here) = cwd {
        if here.join(CONFIG_FILE_NAME).exists() {
            return Ok(PathBuf::from(here));
        }

        cwd = here.parent();
    }

    Err(DtMgrError::FindConfig { cwd: initial.to_owned() })
}

fn parse_config(path_to_dtmgr_toml: impl AsRef<Path>) -> Result<DtMgrConfig, DtMgrError> {
    let content = std::fs::read_to_string(&path_to_dtmgr_toml)
        .map_err(|e| DtMgrError::ReadFile { path: path_to_dtmgr_toml.as_ref().to_owned(), source: e })?;

    toml::from_str(content.as_str())
        .map_err(|e|DtMgrError::ParseConfig { source: e })
}

fn hash_config(config: &DtMgrConfig) -> Result<String, DtMgrError> {
    let mut hasher = Sha3_256::new();
    let config_bytes = postcard::to_stdvec(&config)
        .map_err(|e| DtMgrError::HashConfig { source: e })?;
    hasher.update(config_bytes);
    let hash: [u8; 32] = hasher.finalize().into();
    Ok(hex::encode(hash))
}

fn make_dot_dir(dot_dir: impl AsRef<Path>) -> Result<(), DtMgrError> {
    std::fs::create_dir(&dot_dir)
        .map_err(|e| DtMgrError::CreateDirectory { dir: dot_dir.as_ref().to_owned(), source: e })
}

fn make_config_and_var(dot_dir: impl AsRef<Path>) -> Result<(), DtMgrError> {
    std::fs::create_dir(dot_dir.as_ref().join("texmf-config"))
        .map_err(|e| DtMgrError::CreateDirectory { dir: dot_dir.as_ref().to_owned(), source: e })?;
    std::fs::create_dir(dot_dir.as_ref().join("texmf-var"))
        .map_err(|e| DtMgrError::CreateDirectory { dir: dot_dir.as_ref().to_owned(), source: e })
}

fn make_dot_dir_version_file(dot_dir: impl AsRef<Path>, config: &DtMgrConfig) -> Result<(), DtMgrError> {
    let version_file = dot_dir.as_ref().join("version");
    let config_hash = hash_config(&config)?;
    std::fs::write(&version_file, config_hash)
        .map_err(|e| DtMgrError::WriteFile { file: version_file, source: e })
}

fn build_dependency_tree(config: &DtMgrConfig, tlmgr_platform: impl AsRef<str>) -> Result<Map<String, TlPObjInfo>, DtMgrError> {
    let mut queue: Set<String> = Set::new();
    queue.insert(String::from("texlive.infra"));
    queue.insert(String::from("kpathsea"));

    // TODO check this for other platforms
    if cfg!(windows) {
        queue.insert(String::from("tlperl.windows"));
    }

    for dep in config.dependencies.iter() {
        queue.insert(dep.clone());
    }

    let mut result: Map<String, TlPObjInfo> = Map::new();
    while !queue.is_empty() {
        let info = info_about_packages(&queue)?;
        queue.clear();

        for tlpobjinfo in info.into_iter() {
            if let Some(depends) = &tlpobjinfo.depends {
                for dep in depends.iter() {
                    let true_dep = if dep.ends_with(".ARCH") {
                        &(String::from(&dep[0..dep.len() - ".ARCH".len()]) + "." + tlmgr_platform.as_ref())
                    } else {
                        dep
                    };
                    if !result.contains_key(true_dep) {
                        queue.insert(true_dep.clone());
                    }
                }
            }
            result.insert(tlpobjinfo.name.clone(), tlpobjinfo);
        }
    }

    Ok(result)
}

#[cfg(windows)]
fn create_symlink(target: impl AsRef<Path>, name: impl AsRef<Path>) -> std::io::Result<()> {
    if target.as_ref().is_dir() {
        std::os::windows::fs::symlink_dir(target, name)
    } else {
        std::os::windows::fs::symlink_file(target, name)
    }
}
#[cfg(unix)]
fn create_symlink(target: impl AsRef<Path>, name: impl AsRef<Path>) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, name)
}

fn create_texlive_copy(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, relative: impl AsRef<Path>) -> Result<(), DtMgrError> {
    let full_old = old_root.as_ref().join(&relative);
    let full_new = new_root.as_ref().join(relative);
    let parent_dir = full_new.parent().expect("a path created by a join should have a parent");
    std::fs::create_dir_all(parent_dir)
        .map_err(|e| DtMgrError::CreateDirectory { dir: parent_dir.to_owned(), source: e })?;

    std::fs::copy(full_old, &full_new)
        .map_err(|e| DtMgrError::WriteFile { file: full_new, source: e })?;
    Ok(())
}

fn create_texlive_hardlink(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, relative: impl AsRef<Path>) -> Result<(), DtMgrError> {
    let full_old = old_root.as_ref().join(&relative);
    let full_new = new_root.as_ref().join(relative);
    let parent_dir = full_new.parent().expect("a path created by a join should have a parent");
    std::fs::create_dir_all(parent_dir)
        .map_err(|e| DtMgrError::CreateDirectory { dir: parent_dir.to_owned(), source: e })?;

    match std::fs::hard_link(&full_old, &full_new) {
        Ok(()) => { Ok(()) }
        Err(_) => {
            std::fs::copy(full_old, &full_new)
                .map_err(|e| DtMgrError::WriteFile { file: full_new, source: e })?;
            Ok(())
        }
    }
}

fn create_texlive_symlink(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, relative: impl AsRef<Path>) -> Result<(), DtMgrError> {
    let full_old = old_root.as_ref().join(&relative);
    let full_new = new_root.as_ref().join(relative);
    let parent_dir = full_new.parent().expect("a path created by a join should have a parent");
    std::fs::create_dir_all(parent_dir)
        .map_err(|e| DtMgrError::CreateDirectory { dir: parent_dir.to_owned(), source: e })?;

    create_symlink(&full_old, &full_new)
        .map_err(|e| DtMgrError::CreateSymlink { src: full_old, dst: full_new, source: e })
}

fn do_symlinks(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, platform: impl AsRef<str>, pkg: &TlPObjInfo) -> Result<(), DtMgrError> {
    if let Some(binfiles) = &pkg.binfiles {
        if let Some(arch_binfiles) = binfiles.get(platform.as_ref()) {
            for file in arch_binfiles.iter() {
                let parse = PathBuf::from(file);

                // We need to hardlink or copy because abs_path resolves symbolic links
                if (cfg!(windows) && parse.ends_with("kpsewhich.exe")) || parse.ends_with("kpsewhich") {
                    create_texlive_hardlink(&old_root, &new_root, parse)?;
                } else {
                    create_texlive_symlink(&old_root, &new_root, parse)?;
                }
            }
        }
    }
    if let Some(docfiles) = &pkg.docfiles {
        for file in docfiles.iter() {
            let parse = PathBuf::from(&file.file);
            create_texlive_symlink(&old_root, &new_root, parse)?;
        }
    }
    if let Some(runfiles) = &pkg.runfiles {
        for file in runfiles.iter() {
            let parse = PathBuf::from(file);

            if parse.ends_with("updmap.cfg") {
                // updmap.cfg needs to be copied to be updated with updmap-sys --syncwithtrees
                create_texlive_copy(&old_root, &new_root, parse)?;
            } else if cfg!(windows) && parse.extension().is_some_and(|s| s.to_str() == Some("otf")) {
                // https://github.com/lunarmodules/luafilesystem/issues/184
                create_texlive_hardlink(&old_root, &new_root, parse)?;
            } else {
                create_texlive_symlink(&old_root, &new_root, parse)?;
            }
        }
    }
    // TODO check if this is correct
    if let Some(srcfiles) = &pkg.srcfiles {
        for file in srcfiles.iter() {
            let parse = PathBuf::from(file);
            create_texlive_symlink(&old_root, &new_root, parse)?;
        }
    }

    // TODO currently these are all handled by the tools executed later, but I'm not sure if that's right
    // if let Some(executes) = &pkg.executes {
    //     executes.iter().for_each(|e| {
    //         post_installs.push("executes [".to_owned() + &pkg.name + "]: " + e)
    //     })
    // }
    // if let Some(postactions) = &pkg.postactions {
    //     postactions.iter().for_each(|p| {
    //         post_installs.push("postactions [".to_owned() + &pkg.name + "]: " + p)
    //     })
    // }

    Ok(())
}

fn replace_path_env(old_path_env: impl AsRef<str>, target: impl AsRef<Path>, replacement: impl AsRef<Path>) -> String {
    let mut result = Vec::new();

    // TODO check for non-existence of target
    for part in old_path_env.as_ref().split(PATH_ENV_SEPARATOR) {
        let entry = PathBuf::from(part);
        let new_path = if entry.starts_with(&target) {
            let relative = entry.strip_prefix(&target)
                .expect("strip_prefix failed even though starts_with already checked");
            replacement.as_ref().join(relative)
        } else {
            entry
        };
        result.push(new_path.to_str().expect("path created from str not representable as str").to_owned());
    }

    result.join(PATH_ENV_SEPARATOR)
}

fn run_tool_in_dtmgr<I, S>(exe_and_args: I) -> Result<Command, DtMgrError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr> {
    // TODO move this to function parameter
    let dtmgr_directory = find_dtmgr_directory()?;
    let dot_dir = dtmgr_directory.join(".dtmgr");
    let dot_dir_str = dot_dir.to_str()
        .expect(".dtmgr path should be a str");

    let dot_dir_web2c = dot_dir.join("texmf-dist").join("web2c");
    let dot_dir_web2c_str = dot_dir_web2c.to_str()
        .expect(".dtmgr/texmf-dist/web2c should be a str");

    // TODO move this to function parameter
    let old_root = get_texlive_root()?;

    // TODO maybe this needs a cfg()
    let old_path = std::env::var("PATH")
        .expect("there should be a PATH variable");

    // TODO move this to function parameter
    let new_path = replace_path_env(&old_path, &old_root, &dot_dir);
    let mut cmd = cmd_crossplatform_static_args(exe_and_args);
    cmd.env("PATH", &new_path);

    // TODO move this to function parameter
    let mut texmfcnf = String::new();
    texmfcnf.push_str(dot_dir_str);
    texmfcnf.push(KPSE_SEPARATOR);
    texmfcnf.push_str(dot_dir_web2c_str);
    cmd.env("TEXMFCNF", texmfcnf);

    Ok(cmd)
}

fn run() -> Result<ExitCode, DtMgrError> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Install {} => {
            let dtmgr_directory = find_dtmgr_directory()?;

            let config =
                parse_config(dtmgr_directory.join(CONFIG_FILE_NAME))?;

            let dot_dir = dtmgr_directory.join(".dtmgr");
            if dot_dir.is_dir() {
                let version_file = dot_dir.join("version");
                if version_file.is_file() {
                    let version_contents = std::fs::read_to_string(&version_file)
                        .map_err(|e| DtMgrError::ReadFile { path: version_file.to_owned(), source: e })?;
                    let config_hash = hash_config(&config)?;
                    if version_contents == config_hash {
                        // TODO do actual logging
                        println!("Up-to-date");
                        return Ok(ExitCode::SUCCESS);
                    }
                }

                match std::fs::remove_dir_all(&dot_dir) {
                    Ok(()) => {}
                    Err(e) => return Err(DtMgrError::RemoveDirectory { dir: dot_dir, source: e })
                }
            }

            let root = get_texlive_root()?;
            let platform = get_texlive_platform()?;

            // TODO log progress here
            make_dot_dir(&dot_dir)?;

            install_packages_globally(&config.dependencies)?;

            let dep_tree = build_dependency_tree(&config, &platform)?;
            for tlpobj in dep_tree.values() {
                do_symlinks(&root, &dot_dir, &platform, tlpobj)?;
            }

            make_config_and_var(&dot_dir)?;

            // TODO turn these expects into errors
            run_tool_in_dtmgr(["mktexlsr"])?
                .status().expect("should be able to run mktexlsr");
            run_tool_in_dtmgr(["fmtutil-sys", "--missing"])?
                .status().expect("should be able to run fmtutil-sys --missing");
            run_tool_in_dtmgr(["updmap-sys", "--syncwithtrees"])?
                .status().expect("should be able to run updmap-sys --syncwithtrees");
            run_tool_in_dtmgr(["updmap-sys"])?
                .status().expect("should be able to run updmap-sys");

            make_dot_dir_version_file(&dot_dir, &config)?;

            Ok(ExitCode::SUCCESS)
        }
        Commands::Run { program, args } => {
            let mut cmd = run_tool_in_dtmgr([program].iter().chain(args.iter()))?;
            let status = cmd.status()
                .map_err(|e| DtMgrError::CommandExecution { source: e })?;

            match status.code() {
                Some(code) => Ok(ExitCode::from(code as u8)),
                None => Ok(ExitCode::FAILURE),
            }
        }
    }
}

fn main() -> ExitCode {
    run().unwrap_or_else(|err| {
        eprintln!("{}", err);
        ExitCode::FAILURE
    })
}
