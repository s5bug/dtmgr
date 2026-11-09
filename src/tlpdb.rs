use crate::tlpdb::TlpDbError::ConflictingNames;
use memmap2::{Mmap, MmapOptions};
use ouroboros::self_referencing;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::warn;

#[self_referencing]
#[derive(Debug)]
pub struct TlpDb {
    mmap: Mmap,
    #[borrows(mmap)]
    #[covariant]
    pub packages: BTreeMap<&'this str, TlpDbObj<'this>>,
}

impl TlpDb {
    pub fn has_package(&self, name: impl AsRef<str>) -> bool {
        self.borrow_packages().contains_key(name.as_ref())
    }

    pub fn missing_packages<'a, I, S>(&self, wanted: I) -> BTreeSet<&'a str>
    where
        I: IntoIterator<Item = &'a S>,
        S: AsRef<str> + 'a,
    {
        wanted
            .into_iter()
            .filter(|pkg| !self.has_package(pkg))
            .map(|pkg| pkg.as_ref())
            .collect()
    }

    pub fn closure<'a, I, S>(
        &'a self,
        wanted: I,
        architecture: impl AsRef<str>,
    ) -> BTreeMap<&'a str, &'a TlpDbObj<'a>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let packages = self.borrow_packages();
        let architecture = architecture.as_ref();

        let mut result: BTreeMap<&'a str, &'a TlpDbObj<'a>> = BTreeMap::new();
        let mut stack: Vec<&'a TlpDbObj<'a>> = Vec::new();

        let mut visit_name = |stack: &mut Vec<&'a TlpDbObj<'a>>, name: &str| {
            if let Some((&pkg_key, db_entry)) = packages.get_key_value(name) {
                if result.insert(pkg_key, db_entry).is_none() {
                    stack.push(db_entry);
                }
            } else {
                warn!("package {name} was requested in the closure but it couldn't be found");
            }
        };

        for dep in wanted {
            visit_name(&mut stack, dep.as_ref())
        }

        let mut buf = String::new();
        while let Some(db_entry) = stack.pop() {
            for &dep in &db_entry.depends {
                let true_name = if let Some(generic) = dep.strip_suffix(".ARCH") {
                    buf.clear();
                    buf.reserve(generic.len() + 1 + architecture.len());
                    buf.push_str(generic);
                    buf.push('.');
                    buf.push_str(architecture);

                    &buf
                } else {
                    dep
                };

                visit_name(&mut stack, true_name)
            }
        }

        result
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TlpDbObj<'a> {
    pub name: &'a str,
    pub depends: Vec<&'a str>,
    pub executes: Vec<&'a str>,
    pub runfiles: Vec<&'a str>,
    pub docfiles: Vec<&'a str>,
    pub srcfiles: Vec<&'a str>,
    pub binfiles: BTreeMap<&'a str, Vec<&'a str>>,
}

#[derive(Error, Debug)]
pub enum TlpDbError {
    #[error("failed to open tlpdb file {file}")]
    OpenFile {
        file: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to map tlpdb file {file} into memory")]
    MapFile {
        file: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read tlpdb file {file} as utf8")]
    FromUtf8 {
        file: PathBuf,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("list element ({value}) outside of known section in {package}")]
    ListElementOutsideOfSection { package: String, value: String },
    #[error("list element ({value}) outside of package")]
    ListElementOutsideOfPackage { value: String },
    #[error("expected a key-value pair, instead found: {line}")]
    KeyValuePair { line: String },
    #[error("conflicting package name fields: {current_name} (old) {discovered_name} (new)")]
    ConflictingNames {
        current_name: String,
        discovered_name: String,
    },
    #[error("field {field} found before name declaration")]
    FieldWithoutPackage { field: String },
}

impl TlpDb {
    pub fn from_file(f: impl AsRef<Path>) -> Result<TlpDb, TlpDbError> {
        let file = File::open(&f).map_err(|source| TlpDbError::OpenFile {
            file: f.as_ref().to_owned(),
            source,
        })?;
        let mmap = unsafe { MmapOptions::new().map_copy_read_only(&file) }.map_err(|source| {
            TlpDbError::MapFile {
                file: f.as_ref().to_owned(),
                source,
            }
        })?;

        let tlpdb = TlpDbTryBuilder {
            mmap,
            packages_builder: |mmap: &Mmap| {
                let utf8 = std::str::from_utf8(&mmap).map_err(|source| TlpDbError::FromUtf8 {
                    file: f.as_ref().to_owned(),
                    source,
                })?;
                Self::read_tlpdb(utf8)
            },
        }
        .try_build()?;

        Ok(tlpdb)
    }

    fn read_tlpdb<'a>(file: &'a str) -> Result<BTreeMap<&'a str, TlpDbObj<'a>>, TlpDbError> {
        let mut db = BTreeMap::new();
        let mut current: Option<TlpDbObj<'a>> = None;

        #[derive(Copy, Clone)]
        enum ListParseSection<'a> {
            None,
            Run,
            Doc,
            Src,
            Bin(&'a str),
        }
        let mut section: ListParseSection = ListParseSection::None;

        for line in file.lines() {
            if line.is_empty() {
                // empty line terminates the current package
                if let Some(pkg) = current.take() {
                    db.insert(pkg.name, pkg);
                }
                section = ListParseSection::None;
            } else if let Some(elem) = line.strip_prefix(' ') {
                // an indented line means adding to the current list
                if let Some(pkg) = current.as_mut() {
                    match section {
                        ListParseSection::None => Err(TlpDbError::ListElementOutsideOfSection {
                            package: pkg.name.to_owned(),
                            value: elem.to_owned(),
                        })?,
                        ListParseSection::Run => pkg.runfiles.push(elem),
                        ListParseSection::Doc => {
                            let path = elem.split_once(' ').map_or(elem, |(path, _)| path);
                            pkg.docfiles.push(path)
                        }
                        ListParseSection::Src => pkg.srcfiles.push(elem),
                        ListParseSection::Bin(arch) => {
                            pkg.binfiles.entry(arch).or_default().push(elem)
                        }
                    }
                } else {
                    Err(TlpDbError::ListElementOutsideOfPackage {
                        value: elem.to_owned(),
                    })?
                }
            } else {
                // otherwise, we've ended a list and expect a new section
                section = ListParseSection::None;
                let (k, v) = line
                    .split_once(' ')
                    .ok_or_else(|| TlpDbError::KeyValuePair {
                        line: line.to_owned(),
                    })?;

                match k {
                    "name" => {
                        if let Some(pkg) = current.take() {
                            Err(ConflictingNames {
                                current_name: pkg.name.to_owned(),
                                discovered_name: v.to_owned(),
                            })?
                        }
                        current = Some(TlpDbObj {
                            name: v,
                            ..Default::default()
                        })
                    }
                    k => {
                        let pkg =
                            current
                                .as_mut()
                                .ok_or_else(|| TlpDbError::FieldWithoutPackage {
                                    field: k.to_owned(),
                                })?;
                        match k {
                            "depend" => pkg.depends.push(v),
                            "execute" => pkg.executes.push(v),
                            "runfiles" => section = ListParseSection::Run,
                            "docfiles" => section = ListParseSection::Doc,
                            "srcfiles" => section = ListParseSection::Src,
                            "binfiles" => {
                                if let Some(arch) =
                                    v.split(' ').find_map(|part| part.strip_prefix("arch="))
                                {
                                    section = ListParseSection::Bin(arch)
                                } else {
                                    unreachable!("binfiles lacks an architecture")
                                }
                            }

                            "catalogue"
                            | "postaction"
                            | "relocated"
                            | "category"
                            | "revision"
                            | "containerchecksum"
                            | "containersize"
                            | "shortdesc"
                            | "longdesc"
                            | "doccontainersize"
                            | "doccontainerchecksum"
                            | "srccontainersize"
                            | "srccontainerchecksum" => {}
                            c if c.starts_with("catalogue-") => {}

                            _ => warn!("unexpected tlpdb key: {k}"),
                        }
                    }
                }
            }
        }

        if let Some(pkg) = current.take() {
            db.insert(pkg.name, pkg);
        }

        Ok(db)
    }
}
