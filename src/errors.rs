use crate::tlpdb::TlpDbError;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DtmgrError {
    #[error("unable to parse configuration file `dtmgr.toml`")]
    ParseConfig {
        #[source]
        source: toml::de::Error,
    },
    #[error("unable to read file ({path})")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to hash config")]
    HashConfig {
        #[source]
        source: postcard::Error,
    },
    #[error("system failure executing a command `{command}`")]
    CommandExecution {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("command `{command}` exited with non-zero exit code ({code:?})")]
    CommandStatus { command: String, code: Option<i32> },
    #[error("failed to parse json")]
    JsonParse {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to generate json")]
    JsonSerialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to retrieve current directory")]
    CurrentDirectory {
        #[source]
        source: std::io::Error,
    },
    #[error("unable to find dtmgr.toml in current directory ({cwd}) or any of its parents")]
    FindConfig { cwd: PathBuf },
    #[error("unable to create directory ({dir})")]
    CreateDirectory {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to write to file ({file})")]
    WriteFile {
        file: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to create symlink (src: {src}, dst: {dst})")]
    CreateSymlink {
        src: PathBuf,
        dst: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to copy (src: {src}, dst: {dst})")]
    Copy {
        src: PathBuf,
        dst: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to remove directory ({dir})")]
    RemoveDirectory {
        dir: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to read tlpdb")]
    TlpdbError {
        #[source]
        source: TlpDbError,
    },
    #[error("00texlive.installation in tlpdb lacked a setting_available_architectures dependency")]
    MissingPlatform,
    #[error("could not create texmf-dist/ls-R")]
    CreateLsr {
        #[source]
        source: std::io::Error,
    },
    #[error("could not write to texmf-dist/ls-R")]
    WriteLsr {
        #[source]
        source: std::io::Error,
    },
    #[error("could not create updmap.cfg")]
    CreateUpdmap {
        #[source]
        source: std::io::Error,
    },
    #[error("could not open updmap-hdr.cfg")]
    OpenUpdmapHeader {
        #[source]
        source: std::io::Error,
    },
    #[error("could not copy updmap-hdr.cfg contents to updmap.cfg")]
    CopyHeaderToUpdmap {
        #[source]
        source: std::io::Error,
    },
    #[error("could not write to updmap.cfg")]
    WriteUpdmap {
        #[source]
        source: std::io::Error,
    },
    #[error("could not flush updmap.cfg to disk")]
    FlushUpdmap {
        #[source]
        source: std::io::Error,
    },
    #[error("could not open fmtutil.cnf")]
    OpenFmtutil {
        #[source]
        source: std::io::Error,
    },
    #[error("could not write to fmtutil.cnf")]
    WriteFmtutil {
        #[source]
        source: std::io::Error,
    },
    #[error("could not flush fmtutil.cnf to disk")]
    FlushFmtutil {
        #[source]
        source: std::io::Error,
    },
}
