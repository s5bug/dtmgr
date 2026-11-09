pub mod errors;
pub mod symlink;
pub mod symlink_solver;
mod tlpdb;

use crate::symlink_solver::TexPackageDir;
use crate::tlpdb::{TlpDb, TlpDbObj};
use clap::{Parser, Subcommand};
use digest_io::IoWrapper;
use errors::DtmgrError;
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::collections::BTreeMap as Map;
use std::collections::BTreeSet as Set;
use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

#[cfg(windows)]
const KPSE_SEPARATOR: char = ';';
#[cfg(unix)]
const KPSE_SEPARATOR: char = ':';

const CONFIG_FILE_NAME: &str = "dtmgr.toml";

const TRACING_SUBSCRIBER_VERSION: &str = env!("TRACING_SUBSCRIBER_VERSION");

const AFTER_HELP: &str = const_format::formatcp!(
    r"dtmgr supports the RUST_LOG environment variable to control verbosity
    https://docs.rs/tracing-subscriber/{TRACING_SUBSCRIBER_VERSION}/tracing_subscriber/filter/struct.EnvFilter.html"
);

#[derive(Parser)]
#[command(version, about, long_about = None, arg_required_else_help = true, after_help = AFTER_HELP)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long)]
    nix: bool,
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
    },
}

#[derive(Debug, Deserialize, Serialize, Hash)]
pub struct DtmgrConfig {
    dependencies: Set<String>,
}

#[cfg(windows)]
fn cmd_tlmgr(texmfroot: impl AsRef<Path>, architecture: impl AsRef<str>) -> Command {
    let tlperl_dir = texmfroot.as_ref().join("tlpkg").join("tlperl");

    let tlperl_bin = tlperl_dir.join("bin");
    let tlbin = texmfroot.as_ref().join("bin").join(architecture.as_ref());

    let tlperl = tlperl_bin.join("perl.exe");
    let tlmgr_pl = texmfroot
        .as_ref()
        .join("texmf-dist")
        .join("scripts")
        .join("texlive")
        .join("tlmgr.pl");

    let perl5lib = tlperl_dir.join("lib");
    let old_path = std::env::var("PATH").expect("there should be a PATH variable");
    let path = std::env::join_paths(
        [tlperl_bin, tlbin]
            .into_iter()
            .chain(std::env::split_paths(&old_path)),
    )
    .expect("prepending TeXLive paths to PATH should succeed");

    let mut c = Command::new(tlperl);
    c.arg(tlmgr_pl);
    c.env("PATH", path);
    c.env("PERL5LIB", perl5lib);
    c
}

#[cfg(unix)]
fn cmd_tlmgr(texmfroot: impl AsRef<Path>, architecture: impl AsRef<str>) -> Command {
    Command::new("tlmgr")
}

fn run_command_success(called: impl AsRef<str>, mut cmd: Command) -> Result<(), DtmgrError> {
    let status = cmd.status();

    match status {
        Ok(status) => {
            if status.success() {
                Ok(())
            } else {
                Err(DtmgrError::CommandStatus {
                    command: called.as_ref().to_owned(),
                    code: status.code(),
                })
            }
        }
        Err(source) => Err(DtmgrError::CommandExecution {
            command: called.as_ref().to_owned(),
            source,
        }),
    }
}

fn run_command_success_out(
    called: impl AsRef<str>,
    mut cmd: Command,
) -> Result<std::process::Output, DtmgrError> {
    let out = cmd.output();

    match out {
        Ok(out) => {
            if out.status.success() {
                Ok(out)
            } else {
                Err(DtmgrError::CommandStatus {
                    command: called.as_ref().to_owned(),
                    code: out.status.code(),
                })
            }
        }
        Err(source) => Err(DtmgrError::CommandExecution {
            command: called.as_ref().to_owned(),
            source,
        }),
    }
}

fn get_texlive_root() -> Result<PathBuf, DtmgrError> {
    let mut cmd = Command::new("kpsewhich");
    cmd.arg("-var-value=TEXMFROOT");
    let kpse_out = run_command_success_out("kpsewhich -var-value=TEXMFROOT", cmd)?;

    Ok(PathBuf::from(
        String::from_utf8(kpse_out.stdout)
            .expect("kpsewhich output is utf-8")
            .trim(),
    ))
}

fn get_texlive_platform(tlpdb: &TlpDb) -> Result<String, DtmgrError> {
    let maybe_platform = tlpdb.with_packages(|p| {
        let installation_pkg = p.get("00texlive.installation")?;
        installation_pkg
            .depends
            .iter()
            .find_map(|d| d.strip_prefix("setting_available_architectures:"))
            .map(|r| r.to_owned())
    });
    if let Some(platform) = maybe_platform
        && !platform.contains(' ')
    {
        Ok(platform)
    } else if cfg!(windows) {
        Ok("windows".to_owned())
    } else {
        let mut tlmgr_platform = Command::new("tlmgr");
        tlmgr_platform.arg("print-platform");
        let tlmgr_platform_out = run_command_success_out("tlmgr print-platform", tlmgr_platform)?;
        let platform_string = String::from_utf8(tlmgr_platform_out.stdout)
            .expect("tlmgr print-platform produces utf8");
        Ok(platform_string.trim().to_owned())
    }
}

fn make_updmap_cfg(
    dot_dir: impl AsRef<Path>,
    closure: &Map<&str, &TlpDbObj<'_>>,
) -> Result<(), DtmgrError> {
    let web2c = dot_dir.as_ref().join("texmf-dist").join("web2c");

    let out_path = web2c.join("updmap.cfg");
    let hdr_path = web2c.join("updmap-hdr.cfg");

    let mut out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(out_path)
        .map_err(|source| DtmgrError::CreateUpdmap { source })?;
    let mut hdr_file =
        File::open(hdr_path).map_err(|source| DtmgrError::OpenUpdmapHeader { source })?;
    std::io::copy(&mut hdr_file, &mut out_file)
        .map_err(|source| DtmgrError::CopyHeaderToUpdmap { source })?;

    let mut out_writer = BufWriter::new(out_file);
    let write_error = |source| DtmgrError::WriteUpdmap { source };
    writeln!(out_writer).map_err(write_error)?;

    for &pkg in closure.values() {
        for &exec in &pkg.executes {
            if let Some(map) = exec.strip_prefix("addMap ") {
                writeln!(out_writer, "Map {}", map.trim()).map_err(write_error)?;
            } else if let Some(map) = exec.strip_prefix("addMixedMap ") {
                writeln!(out_writer, "MixedMap {}", map.trim()).map_err(write_error)?;
            } else if let Some(map) = exec.strip_prefix("addKanjiMap ") {
                writeln!(out_writer, "KanjiMap {}", map.trim()).map_err(write_error)?;
            }
        }
    }

    out_writer
        .flush()
        .map_err(|source| DtmgrError::FlushUpdmap { source })
}

fn make_fmtutil_cnf(
    dot_dir: impl AsRef<Path>,
    closure: &Map<&str, &TlpDbObj<'_>>,
) -> Result<(), DtmgrError> {
    let out_path = dot_dir
        .as_ref()
        .join("texmf-dist")
        .join("web2c")
        .join("fmtutil.cnf");
    let out_file = File::create(out_path).map_err(|source| DtmgrError::OpenFmtutil { source })?;
    let mut out_writer = BufWriter::new(out_file);

    for &pkg in closure.values() {
        for &exec in &pkg.executes {
            let Some(rest) = exec.strip_prefix("AddFormat ") else {
                continue;
            };

            let mut name = "";
            let mut engine = "";
            let mut patterns = "-";
            let mut options = "";
            let mut disabled = false;

            let mut rest = rest.trim();
            while !rest.is_empty() {
                // parse one k=v pair
                let Some((k, after_eq)) = rest.split_once('=') else {
                    warn!("malformed AddFormat entry {rest}");
                    break;
                };
                // turn k = v or k="v" into (k, v)
                let k = k.trim();
                let (v, remainder) = if let Some(quoted) = after_eq.strip_prefix('"') {
                    quoted.split_once('"').unwrap_or((quoted, ""))
                } else {
                    after_eq
                        .split_once(|c: char| c.is_ascii_whitespace())
                        .unwrap_or((after_eq, ""))
                };
                // consume the k=v pair
                rest = remainder.trim_start();

                match k {
                    "name" => name = v,
                    "engine" => engine = v,
                    "patterns" if !v.is_empty() => patterns = v,
                    "options" => options = v,
                    "mode" if v == "disabled" => disabled = true,
                    "fmttriggers" => {}
                    _ => warn!("not sure how to handle AddFormat entry {k}={v}"),
                }
            }

            if !name.is_empty() && !engine.is_empty() && !disabled {
                writeln!(out_writer, "{} {} {} {}", name, engine, patterns, options)
                    .map_err(|source| DtmgrError::WriteFmtutil { source })?;
            }
        }
    }

    out_writer
        .flush()
        .map_err(|source| DtmgrError::FlushFmtutil { source })
}

#[derive(Debug)]
enum DtmgrInputs {
    Tlmgr {
        texlive_root: PathBuf,
        architecture: String,
        tlpdb: TlpDb,
    },
    Nix,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum DtmgrHashResult {
    Match,
    NoMatch,
    Missing,
}

impl DtmgrInputs {
    pub fn tlmgr() -> Result<DtmgrInputs, DtmgrError> {
        let texlive_root = get_texlive_root()?;
        let tlpdb_path = texlive_root.join("tlpkg").join("texlive.tlpdb");
        let tlpdb =
            TlpDb::from_file(tlpdb_path).map_err(|source| DtmgrError::TlpdbError { source })?;
        let architecture = get_texlive_platform(&tlpdb)?;
        Ok(DtmgrInputs::Tlmgr {
            texlive_root,
            tlpdb,
            architecture,
        })
    }

    pub fn nix() -> Result<DtmgrInputs, DtmgrError> {
        Ok(DtmgrInputs::Nix)
    }

    pub fn hash_state(&self, config: &DtmgrConfig) -> Result<String, DtmgrError> {
        let mut hasher = IoWrapper(Sha3_256::new());

        if let Self::Tlmgr { tlpdb, .. } = self {
            _ = tlpdb
                .with_packages(|p| postcard::to_io(&p, &mut hasher))
                .map_err(|source| DtmgrError::HashConfig { source })?;
        }

        _ = postcard::to_io(&config, &mut hasher)
            .map_err(|source| DtmgrError::HashConfig { source })?;

        let hash: [u8; 32] = hasher.0.finalize().into();
        Ok(hex::encode(hash))
    }

    pub fn check_hash(
        &self,
        dot_dir: impl AsRef<Path>,
        config: &DtmgrConfig,
    ) -> Result<DtmgrHashResult, DtmgrError> {
        let version_file = dot_dir.as_ref().join("version");
        let version_contents = match std::fs::read_to_string(&version_file) {
            Ok(contents) => contents,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DtmgrHashResult::Missing);
            }
            Err(source) => {
                return Err(DtmgrError::ReadFile {
                    path: version_file,
                    source,
                });
            }
        };

        let config_hash = self.hash_state(config)?;
        if version_contents == config_hash {
            Ok(DtmgrHashResult::Match)
        } else {
            Ok(DtmgrHashResult::NoMatch)
        }
    }

    pub fn write_hash(
        &self,
        dot_dir: impl AsRef<Path>,
        config: &DtmgrConfig,
    ) -> Result<(), DtmgrError> {
        let version_file = dot_dir.as_ref().join("version");
        let config_hash = self.hash_state(&config)?;
        std::fs::write(&version_file, config_hash).map_err(|e| DtmgrError::WriteFile {
            file: version_file,
            source: e,
        })
    }

    pub fn check_isolation(&self, dot_dir: impl AsRef<Path>) -> Result<(), DtmgrError> {
        const TEXMF_VARS_TO_CHECK: [&'static str; 8] = [
            "TEXMF",
            "TEXMFDBS",
            "TEXMFSYSVAR",
            "TEXMFSYSCONFIG",
            "TEXMFVAR",
            "TEXMFCONFIG",
            "VARTEXFONTS",
            "TEXMFCACHE",
        ];

        let dot_dir = dot_dir.as_ref();
        let query = TEXMF_VARS_TO_CHECK
            .map(|v| format!("${v}"))
            .join(&KPSE_SEPARATOR.to_string());

        let cmd = self.run_tool(
            dot_dir,
            ["kpsewhich".to_owned(), format!("-expand-braces={query}")],
        )?;
        let out = run_command_success_out("kpsewhich -expand-braces=...", cmd)?;
        let stdout = String::from_utf8(out.stdout).expect("kpsewhich output is utf-8");
        for entry in stdout
            .trim()
            .split(KPSE_SEPARATOR)
            .filter(|e| !e.is_empty())
        {
            // !!X or X/ or !!X/ becomes X
            let path = entry.trim_start_matches("!!").trim_end_matches('/');
            // if this variable doesn't point inside .dtmgr/, we have a problem
            if !Path::new(path).starts_with(dot_dir) {
                return Err(DtmgrError::NotIsolated {
                    path: PathBuf::from(path),
                });
            }
        }
        Ok(())
    }

    pub fn install(
        &mut self,
        config: &DtmgrConfig,
        dot_dir: impl AsRef<Path>,
    ) -> Result<(), DtmgrError> {
        match self {
            DtmgrInputs::Tlmgr {
                tlpdb,
                texlive_root,
                architecture,
            } => {
                let missing = tlpdb.missing_packages(&config.dependencies);
                if !missing.is_empty() {
                    let info = missing.iter().copied().collect::<Vec<_>>().join(", ");
                    info!(
                        "the following requested packages are not present on your system: {}",
                        info
                    );
                    info!("they will be installed via `tlmgr install`...");

                    match install_packages_globally(
                        &texlive_root,
                        &architecture,
                        &config.dependencies,
                    ) {
                        Err(DtmgrError::CommandStatus { code: Some(2), .. }) => {
                            warn!(
                                "not performing install because tlmgr seems to need sudo. continuing..."
                            )
                        }
                        other => other?,
                    };

                    // after installing packages, it's possible that the tlpdb needs to be reparsed
                    *tlpdb = TlpDb::from_file(texlive_root.join("tlpkg").join("texlive.tlpdb"))
                        .map_err(|source| DtmgrError::TlpdbError { source })?;
                }

                let closure = build_dependency_tree(config, tlpdb, &architecture);
                let tpd = TexPackageDir::from_wanted(tlpdb, &architecture, &closure);

                tpd.do_symlinking(texlive_root, &dot_dir)?;

                make_texmf_cnf(&dot_dir)?;

                make_config_and_var(&dot_dir)?;

                tpd.make_dist_lsr(&dot_dir)?;
                make_updmap_cfg(&dot_dir, &closure)?;
                make_fmtutil_cnf(&dot_dir, &closure)?;

                self.check_isolation(&dot_dir)?;

                let final_fmtutil = self.run_tool(
                    &dot_dir,
                    ["fmtutil-sys", "--missing", "--no-strict", "--nohash"],
                )?;
                _ = run_command_success(
                    "fmtutil-sys --missing --no-strict --nohash",
                    final_fmtutil,
                )?;

                let final_updmap_sys = self.run_tool(&dot_dir, ["updmap-sys", "--nohash"])?;
                _ = run_command_success("updmap-sys --nohash", final_updmap_sys)?;

                Ok(())
            }
            DtmgrInputs::Nix => {
                let dependencies_json = serde_json::to_string(&config.dependencies)
                    .map_err(|source| DtmgrError::JsonSerialize { source })?;

                let nix_expr = /* language=nix */ r#"
{
    pkgs ? import <nixpkgs> {},
    dependencies
}:
    pkgs.texlive.withPackages (ps: map (n: ps.${n}) (builtins.fromJSON dependencies) )
"#;

                let out_link = dot_dir.as_ref().join("nix-texlive");
                let out_link_str = out_link
                    .to_str()
                    .expect(".dtmgr/nix-texlive should be a str");

                let mut cmd = Command::new("nix");
                cmd.args([
                    "--extra-experimental-features",
                    "nix-command flakes",
                    "build",
                    "--impure",
                    "--out-link",
                    out_link_str,
                    "--argstr",
                    "dependencies",
                    &dependencies_json,
                    "--expr",
                    nix_expr,
                ]);
                _ = run_command_success("nix build ...", cmd)?;

                make_config_and_var(&dot_dir)
            }
        }
    }

    pub fn run_tool<I, S>(
        &self,
        dot_dir: impl AsRef<Path>,
        exe_and_args: I,
    ) -> Result<Command, DtmgrError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let dot_dir = dot_dir.as_ref();
        let dot_dir_str = dot_dir.to_str().expect(".dtmgr path should be a str");

        // TODO route this back to powershell
        let mut exe_args_iter = exe_and_args.into_iter();
        let mut cmd = Command::new(exe_args_iter.next().expect("there's an exe"));
        cmd.args(exe_args_iter);

        // TODO maybe this needs a cfg()
        let old_path = std::env::var("PATH").expect("there should be a PATH variable");

        // TODO move this to function parameter
        let new_path = if let DtmgrInputs::Tlmgr {
            texlive_root,
            architecture,
            ..
        } = self
        {
            // otherwise we have to hide the system installation of TeX utilities
            let old_bin = texlive_root.join("bin").join(architecture);
            let new_bin = dot_dir.join("bin").join(architecture);
            replace_path_env(&old_path, &old_bin, &new_bin)
        } else {
            // on Nix, we assume no system-level installation of TeX Live
            let texlive_bin = dot_dir.join("nix-texlive").join("bin");

            let mut paths = vec![texlive_bin];
            paths.extend(std::env::split_paths(&old_path));
            std::env::join_paths(paths)
                .expect("prepending a path to PATH should result in a valid PATH")
        };
        cmd.env("PATH", &new_path);

        let texmf_var = dot_dir.join("texmf-var");
        let texmf_config = dot_dir.join("texmf-config");
        cmd.env("TEXMFVAR", &texmf_var);
        cmd.env("TEXMFCONFIG", &texmf_config);

        if let DtmgrInputs::Tlmgr { .. } = self {
            // outside a Nix environment, we have to force tools to search the right place for other tools
            let dot_dir_web2c = dot_dir.join("texmf-dist").join("web2c");
            let dot_dir_web2c_str = dot_dir_web2c
                .to_str()
                .expect(".dtmgr/texmf-dist/web2c should be a str");

            // TODO move this to function parameter
            let mut texmfcnf = String::new();
            texmfcnf.push_str(dot_dir_str);
            texmfcnf.push(KPSE_SEPARATOR);
            texmfcnf.push_str(dot_dir_web2c_str);
            cmd.env("TEXMFCNF", texmfcnf);
        }

        // for luaotfload
        let xdg_config = dot_dir.join("xdg-config");
        cmd.env("XDG_CONFIG_HOME", &xdg_config);

        // don't index OS fonts, instead require them in the project
        let dot_dir_nonexistent =
            dot_dir.join(".if-this-directory-exists-it-needs-to-be-empty-or-dtmgr-will-not-work");
        cmd.env("OSFONTDIR", &dot_dir_nonexistent);

        // prevent ~/texmf from having an effect
        cmd.env("TEXMFHOME", &dot_dir_nonexistent);

        Ok(cmd)
    }
}

fn install_packages_globally<I, S>(
    texmfroot: impl AsRef<Path>,
    architecture: impl AsRef<str>,
    packages: I,
) -> Result<(), DtmgrError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    // take ownership of the iterator data
    let packages: Vec<S> = packages.into_iter().collect();
    // create a vec of string borrows
    let args: Vec<&str> = packages.iter().map(S::as_ref).collect();

    let mut cmd = cmd_tlmgr(texmfroot, architecture);
    cmd.arg("install");
    cmd.args(&args);
    _ = run_command_success(format!("tlmgr install {}", args.join(" ")), cmd)?;
    Ok(())
}

fn find_dtmgr_directory() -> Result<PathBuf, DtmgrError> {
    let initial =
        std::env::current_dir().map_err(|source| DtmgrError::CurrentDirectory { source })?;

    initial
        .ancestors()
        .find(|dir| dir.join(CONFIG_FILE_NAME).is_file())
        .map(Path::to_path_buf)
        .ok_or(DtmgrError::FindConfig { cwd: initial })
}

fn parse_config(path_to_dtmgr_toml: impl AsRef<Path>) -> Result<DtmgrConfig, DtmgrError> {
    let content =
        std::fs::read_to_string(&path_to_dtmgr_toml).map_err(|e| DtmgrError::ReadFile {
            path: path_to_dtmgr_toml.as_ref().to_owned(),
            source: e,
        })?;

    toml::from_str(content.as_str()).map_err(|e| DtmgrError::ParseConfig { source: e })
}

fn make_dot_dir(dot_dir: impl AsRef<Path>) -> Result<(), DtmgrError> {
    std::fs::create_dir(&dot_dir).map_err(|e| DtmgrError::CreateDirectory {
        dir: dot_dir.as_ref().to_owned(),
        source: e,
    })
}

fn make_texmf_cnf(dot_dir: impl AsRef<Path>) -> Result<(), DtmgrError> {
    let root_unix_slashes = dot_dir
        .as_ref()
        .components()
        .map(|c| c.as_os_str().to_str().expect("paths are utf8"))
        .collect::<Vec<_>>()
        .join("/");
    let texmf_cnf_file = dot_dir.as_ref().join("texmf.cnf");

    let cnf = format!(
        r#"
TEXMFROOT = {root_unix_slashes}
TEXMFDIST = $TEXMFROOT/texmf-dist
TEXMFMAIN = $TEXMFDIST
TEXMFLOCAL = $TEXMFROOT/texmf-local
TEXMFSYSVAR = $TEXMFROOT/texmf-var
TEXMFSYSCONFIG = $TEXMFROOT/texmf-config
TEXMF = {{$TEXMFCONFIG,$TEXMFVAR,$TEXMFHOME,!!$TEXMFLOCAL,!!$TEXMFSYSCONFIG,!!$TEXMFSYSVAR,!!$TEXMFDIST}}
TEXMFDBS = {{!!$TEXMFLOCAL,!!$TEXMFSYSCONFIG,!!$TEXMFSYSVAR,!!$TEXMFDIST}}
VARTEXFONTS = $TEXMFVAR/fonts
TEXMFCACHE = $TEXMFVAR
"#
    );

    std::fs::write(&texmf_cnf_file, cnf).map_err(|source| DtmgrError::WriteFile {
        file: texmf_cnf_file,
        source,
    })
}

fn make_config_and_var(dot_dir: impl AsRef<Path>) -> Result<(), DtmgrError> {
    let texmf_config = dot_dir.as_ref().join("texmf-config");
    std::fs::create_dir(&texmf_config).map_err(|e| DtmgrError::CreateDirectory {
        dir: texmf_config,
        source: e,
    })?;
    let texmf_var = dot_dir.as_ref().join("texmf-var");
    std::fs::create_dir(&texmf_var).map_err(|e| DtmgrError::CreateDirectory {
        dir: texmf_var,
        source: e,
    })?;

    // luaotfload parses $XDG_CONFIG_HOME/luaotfload/luaotfload.conf
    let xdg_config_luaotfload = dot_dir.as_ref().join("xdg-config").join("luaotfload");
    std::fs::create_dir_all(&xdg_config_luaotfload).map_err(|e| DtmgrError::CreateDirectory {
        dir: xdg_config_luaotfload.clone(),
        source: e,
    })?;

    // see luaotfload/doc/luaotfload.conf.rst
    let luaotfload_conf = "[db]\nlocation-precedence = texmf\n";

    let luaotfload_conf_path = xdg_config_luaotfload.join("luaotfload.conf");
    std::fs::write(&luaotfload_conf_path, luaotfload_conf).map_err(|e| DtmgrError::WriteFile {
        file: luaotfload_conf_path,
        source: e,
    })?;

    Ok(())
}

fn build_dependency_tree<'a>(
    config: &DtmgrConfig,
    tlpdb: &'a TlpDb,
    architecture: impl AsRef<str>,
) -> Map<&'a str, &'a TlpDbObj<'a>> {
    // TODO reduce the amount of cloning here
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

    tlpdb.closure(queue, architecture)
}

fn replace_path_env(
    old_path_env: impl AsRef<OsStr>,
    target: impl AsRef<Path>,
    replacement: impl AsRef<Path>,
) -> OsString {
    let mut result = Vec::new();

    // TODO check for non-existence of target
    let mut found = false;
    for part in std::env::split_paths(old_path_env.as_ref()) {
        let entry = PathBuf::from(part);
        let new_path = if let Ok(relative) = entry.strip_prefix(&target) {
            found = true;
            replacement.as_ref().join(relative)
        } else {
            entry
        };
        result.push(new_path.into_os_string());
    }
    if !found {
        result.insert(0, replacement.as_ref().to_owned().into_os_string());
    }

    std::env::join_paths(result).expect("replacing path shouldn't create invalid path")
}

fn run() -> Result<ExitCode, DtmgrError> {
    let cli = Cli::parse();

    let dtmgr_directory = find_dtmgr_directory()?;

    let config = parse_config(dtmgr_directory.join(CONFIG_FILE_NAME))?;

    let dot_dir = dtmgr_directory.join(".dtmgr");

    match cli.command {
        Commands::Install {} => {
            let mut dtmgr_inputs = if cli.nix {
                DtmgrInputs::nix()?
            } else {
                DtmgrInputs::tlmgr()?
            };

            if dtmgr_inputs.check_hash(&dot_dir, &config)? == DtmgrHashResult::Match {
                info!("up-to-date");
                return Ok(ExitCode::SUCCESS);
            }

            match std::fs::remove_dir_all(&dot_dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(DtmgrError::RemoveDirectory {
                        dir: dot_dir,
                        source: e,
                    });
                }
            }

            // TODO log progress here
            make_dot_dir(&dot_dir)?;

            dtmgr_inputs.install(&config, &dot_dir)?;

            dtmgr_inputs.write_hash(&dot_dir, &config)?;

            Ok(ExitCode::SUCCESS)
        }
        Commands::Run { program, args } => {
            let dtmgr_inputs = if cli.nix {
                DtmgrInputs::nix()?
            } else {
                DtmgrInputs::tlmgr()?
            };

            match dtmgr_inputs.check_hash(&dot_dir, &config)? {
                DtmgrHashResult::Match => {}
                DtmgrHashResult::NoMatch => {
                    warn!("`dtmgr install` appears to be outdated from the current config");
                    warn!("continuing anyway...");
                }
                DtmgrHashResult::Missing => {
                    error!("could not find dtmgr data: did you remember to run `dtmgr install`?");
                    return Ok(ExitCode::FAILURE);
                }
            }

            let mut cmd =
                dtmgr_inputs.run_tool(&dot_dir, std::iter::once(&program).chain(args.iter()))?;
            let status = cmd.status().map_err(|e| DtmgrError::CommandExecution {
                command: program,
                source: e,
            })?;

            match status.code() {
                Some(code) => Ok(ExitCode::from(code as u8)),
                None => Ok(ExitCode::FAILURE),
            }
        }
    }
}

fn main() -> miette::Result<ExitCode> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_ansi_sanitization(false)
        .init();

    run().into_diagnostic()
}
