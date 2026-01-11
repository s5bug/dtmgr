use std::collections::BTreeMap as Map;
use std::collections::BTreeSet as Set;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};

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

fn get_texlive_root() -> PathBuf {
    let kpse_out = cmd_crossplatform_static_args(["kpsewhich", "-var-value=TEXMFROOT"])
        .output().expect("should be able to run kpsewhich to find current texlive root");

    PathBuf::from(String::from_utf8(kpse_out.stdout).expect("kpsewhich output is utf-8").trim())
}

fn get_texlive_platform() -> String {
    let tlmgr_out = cmd_crossplatform_static_args(["tlmgr", "print-platform"])
        .output().expect("should be able to run tlmgr to find current platform");

    String::from_utf8(tlmgr_out.stdout).expect("tlmgr output is utf-8").trim().to_owned()
}

fn install_packages_globally<'a, I, S>(packages: I) -> bool
where
    I: IntoIterator<Item = &'a S>,
    S: AsRef<str> + 'a {
    let mut cmd = cmd_crossplatform_static_args(["tlmgr", "install"].into_iter().chain(packages.into_iter().map(|s| s.as_ref())));
    let out = cmd.status().expect("tlmgr can be run");
    out.success()
}

fn info_about_packages<'a, I, S>(packages: I) -> Vec<TlPObjInfo>
where
    I: IntoIterator<Item = &'a S>,
    S: AsRef<str> + 'a {
    let mut cmd = cmd_crossplatform_static_args(["tlmgr", "info", "--json"].into_iter().chain(packages.into_iter().map(|s| s.as_ref())));
    let out = cmd.output().expect("tlmgr can be run");
    let json = serde_json::from_slice::<Vec<TlPObjInfo>>(out.stdout.as_slice())
        .expect("tlmgr should output a list of tlpobjinfo");
    json
}

fn find_dtmgr_directory() -> Option<PathBuf> {
    let mut cwd: Option<&Path> = Some(&*std::env::current_dir()
        .expect("need to be able to access current dir"));

    while let Some(here) = cwd {
        if here.join(CONFIG_FILE_NAME).exists() {
            return Some(PathBuf::from(here));
        }

        cwd = here.parent();
    }

    None
}

fn parse_config(path_to_dtmgr_toml: impl AsRef<Path>) -> DtMgrConfig {
    let content = std::fs::read_to_string(path_to_dtmgr_toml)
        .expect("toml should be readable");

    toml::from_str(content.as_str()).expect("toml should be a dtmgr config")
}

fn hash_config(config: &DtMgrConfig) -> String {
    let mut hasher = Sha3_256::new();
    let config_bytes = postcard::to_stdvec(&config)
        .expect("config should be able to become a bytevec");
    hasher.update(config_bytes);
    let hash: [u8; 32] = hasher.finalize().into();
    hex::encode(hash)
}

fn make_dot_dir(dot_dir: impl AsRef<Path>) {
    std::fs::create_dir(&dot_dir)
        .expect("we should be able to make .dtmgr");
}

fn make_config_and_var(dot_dir: impl AsRef<Path>) {
    std::fs::create_dir(dot_dir.as_ref().join("texmf-config"))
        .expect("we should be able to make .dtmgr/texmf-config");
    std::fs::create_dir(dot_dir.as_ref().join("texmf-var"))
        .expect("we should be able to make .dtmgr/texmf-var");
}

fn make_dot_dir_version_file(dot_dir: impl AsRef<Path>, config: &DtMgrConfig) {
    let version_file = dot_dir.as_ref().join("version");
    let config_hash = hash_config(&config);
    std::fs::write(version_file, config_hash)
        .expect("we should be able to write version file");
}

fn build_dependency_tree(config: &DtMgrConfig, tlmgr_platform: impl AsRef<str>) -> Map<String, TlPObjInfo> {
    let mut queue: Set<String> = Set::new();
    queue.insert(String::from("texlive.infra"));
    queue.insert(String::from("kpathsea"));

    // TODO check this for other platforms
    if cfg!(windows) {
        queue.insert(String::from("tlperl.windows"));
    }

    config.dependencies.iter().for_each(|s| { queue.insert(s.clone()); });

    let mut result: Map<String, TlPObjInfo> = Map::new();
    while !queue.is_empty() {
        let info = info_about_packages(&queue);
        queue.clear();

        info.into_iter().for_each(|tlpobjinfo| {
            if let Some(depends) = &tlpobjinfo.depends {
                depends.iter().for_each(|dep| {
                    let true_dep = if dep.ends_with(".ARCH") {
                        &(String::from(&dep[0..dep.len() - ".ARCH".len()]) + "." + tlmgr_platform.as_ref())
                    } else {
                        dep
                    };
                    if !result.contains_key(true_dep) {
                        queue.insert(true_dep.clone());
                    }
                })
            }
            result.insert(tlpobjinfo.name.clone(), tlpobjinfo);
        });
    }

    result
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

fn create_texlive_copy(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, relative: impl AsRef<Path>) -> std::io::Result<()> {
    let full_old = old_root.as_ref().join(&relative);
    let full_new = new_root.as_ref().join(relative);
    std::fs::create_dir_all(full_new.parent().expect("a path to a file should have a parent"))?;

    std::fs::copy(full_old, full_new)?;
    Ok(())
}

fn create_texlive_hardlink(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, relative: impl AsRef<Path>) -> std::io::Result<()> {
    let full_old = old_root.as_ref().join(&relative);
    let full_new = new_root.as_ref().join(relative);
    std::fs::create_dir_all(full_new.parent().expect("a path to a file should have a parent"))?;

    match std::fs::hard_link(&full_old, &full_new) {
        Ok(()) => { Ok(()) }
        Err(_) => {
            std::fs::copy(full_old, full_new)?;
            Ok(())
        }
    }
}

fn create_texlive_symlink(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, relative: impl AsRef<Path>) -> std::io::Result<()> {
    let full_old = old_root.as_ref().join(&relative);
    let full_new = new_root.as_ref().join(relative);
    std::fs::create_dir_all(full_new.parent().expect("a path to a file should have a parent"))?;
    create_symlink(full_old, full_new)
}

fn do_symlinks(old_root: impl AsRef<Path>, new_root: impl AsRef<Path>, platform: impl AsRef<str>, pkg: &TlPObjInfo) {
    if let Some(binfiles) = &pkg.binfiles {
        if let Some(arch_binfiles) = &binfiles.get(platform.as_ref()) {
            arch_binfiles.iter().for_each(|file| {
                let parse = PathBuf::from(file);

                // We need to hardlink or copy because abs_path resolves symbolic links
                if (cfg!(windows) && parse.ends_with("kpsewhich.exe")) || parse.ends_with("kpsewhich") {
                    create_texlive_hardlink(&old_root, &new_root, parse)
                        .expect("should be able to hardlink/copy old kpsewhich to new kpsewhich")
                } else {
                    create_texlive_symlink(&old_root, &new_root, parse)
                        .expect("should be able to symlink old file to new file");
                }
            })
        }
    }
    if let Some(docfiles) = &pkg.docfiles {
        docfiles.iter().for_each(|file| {
            let parse = PathBuf::from(&file.file);
            create_texlive_symlink(&old_root, &new_root, parse)
                .expect("should be able to symlink old file to new file");
        });
    }
    if let Some(runfiles) = &pkg.runfiles {
        runfiles.iter().for_each(|file| {
            let parse = PathBuf::from(file);

            if parse.ends_with("updmap.cfg") {
                // updmap.cfg needs to be copied to be updated with updmap-sys --syncwithtrees
                create_texlive_copy(&old_root, &new_root, parse)
                    .expect("should be able to copy old updmap.cfg to new updmap.cfg");
            } else if cfg!(windows) && parse.extension().is_some_and(|s| s.to_str() == Some("otf")) {
                // https://github.com/lunarmodules/luafilesystem/issues/184
                create_texlive_hardlink(&old_root, &new_root, parse)
                    .expect("should be able to hardlink old .otf to new .otf");
            } else {
                create_texlive_symlink(&old_root, &new_root, parse)
                    .expect("should be able to symlink old file to new file");
            }
        });
    }
    // TODO check if this is correct
    if let Some(srcfiles) = &pkg.srcfiles {
        srcfiles.iter().for_each(|file| {
            let parse = PathBuf::from(file);
            create_texlive_symlink(&old_root, &new_root, parse)
                .expect("should be able to symlink old file to new file");
        })
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
}

fn replace_path_env(old_path_env: impl AsRef<str>, target: impl AsRef<Path>, replacement: impl AsRef<Path>) -> String {
    let mut result = Vec::new();

    old_path_env.as_ref().split(PATH_ENV_SEPARATOR).for_each(|part| {
        let entry = PathBuf::from(part);
        let new_path = if entry.starts_with(&target) {
            replacement.as_ref().join(entry.strip_prefix(&target).expect("checked starts_with"))
        } else {
            entry
        };
        result.push(new_path.to_str().expect("path representable as str").to_owned())
    });

    result.join(PATH_ENV_SEPARATOR)
}

fn run_tool_in_dtmgr<I, S>(exe_and_args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr> {
    let dtmgr_directory = find_dtmgr_directory()
        .expect("we should be in a dtmgr location");
    let dot_dir = dtmgr_directory.join(".dtmgr");
    let dot_dir_str = dot_dir.to_str()
        .expect(".dtmgr path should be a str");

    let dot_dir_web2c = dot_dir.join("texmf-dist").join("web2c");
    let dot_dir_web2c_str = dot_dir_web2c.to_str()
        .expect(".dtmgr/texmf-dist/web2c should be a str");

    let old_root = get_texlive_root();

    // TODO maybe this needs a cfg()
    let old_path = std::env::var("PATH")
        .expect("there should be a PATH variable");

    let new_path = replace_path_env(&old_path, &old_root, &dot_dir);
    let mut cmd = cmd_crossplatform_static_args(exe_and_args);
    cmd.env("PATH", &new_path);

    let mut texmfcnf = String::new();
    texmfcnf.push_str(dot_dir_str);
    texmfcnf.push(KPSE_SEPARATOR);
    texmfcnf.push_str(dot_dir_web2c_str);
    cmd.env("TEXMFCNF", texmfcnf);

    cmd
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Install {} => {
            let dtmgr_directory = find_dtmgr_directory()
                .expect("we should be in a dtmgr location");

            let config =
                parse_config(dtmgr_directory.join(CONFIG_FILE_NAME));

            let dot_dir = dtmgr_directory.join(".dtmgr");
            if dot_dir.is_dir() {
                let version_file = dot_dir.join("version");
                if version_file.is_file() {
                    let version_contents = std::fs::read_to_string(version_file)
                        .expect("we should be able to read version file");
                    let config_hash = hash_config(&config);
                    if version_contents == config_hash {
                        // TODO do actual logging
                        println!("Up-to-date");
                        return;
                    }
                }

                std::fs::remove_dir_all(&dot_dir)
                    .expect("could remove old .dtmgr");
            }

            let root = get_texlive_root();
            let platform = get_texlive_platform();

            // TODO log progress here
            make_dot_dir(&dot_dir);

            let install_success = install_packages_globally(&config.dependencies);

            if !install_success {
                eprintln!("Something bad happened");
                // TODO return bad exit code
                return;
            }

            let dep_tree = build_dependency_tree(&config, &platform);
            dep_tree.values().for_each(|tlpobj| {
                do_symlinks(&root, &dot_dir, &platform, tlpobj);
            });

            make_config_and_var(&dot_dir);

            run_tool_in_dtmgr(["mktexlsr"])
                .status().expect("should be able to run mktexlsr");
            run_tool_in_dtmgr(["fmtutil-sys", "--missing"])
                .status().expect("should be able to run fmtutil-sys --missing");
            run_tool_in_dtmgr(["updmap-sys", "--syncwithtrees"])
                .status().expect("should be able to run updmap-sys --syncwithtrees");
            run_tool_in_dtmgr(["updmap-sys"])
                .status().expect("should be able to run updmap-sys");

            make_dot_dir_version_file(&dot_dir, &config);
        }
        Commands::Run { program, args } => {
            let mut cmd = run_tool_in_dtmgr([program].iter().copied().chain(args.iter()));
            cmd.status().expect("should be able to run command");
            // TODO propagate status
        }
    }
}
