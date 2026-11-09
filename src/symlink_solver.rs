use crate::errors::DtmgrError;
use crate::symlink;
use crate::symlink::create_file_symlink;
use crate::tlpdb::{TlpDb, TlpDbObj};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Default, Debug)]
pub struct TexPackageDir<'a> {
    children: BTreeMap<&'a str, Result<TexPackageDir<'a>, TexPackageFile>>,
    has_unwanted: bool,
    has_special: bool,
}

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub enum TexPackageFile {
    #[default]
    Symlink,
    Hardlink,
    Copy,
    Skip,
}

// see luaotfload/src/luaotfload-database.lua
// FIXME https://github.com/lunarmodules/luafilesystem/issues/184
const FONT_EXTENSIONS: [&str; 5] = ["otf", "ttc", "ttf", "afm", "pfb"];

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Relocation {
    Relocatable,
    Fixed,
}

impl<'a> TexPackageDir<'a> {
    pub fn from_wanted(
        whole_database: &'a TlpDb,
        architecture: impl AsRef<str>,
        closure: &BTreeMap<&'a str, &'a TlpDbObj<'a>>,
    ) -> TexPackageDir<'a> {
        let mut root = TexPackageDir::default();
        for (&k, &v) in closure {
            // we must mark every file in the closure as wanted

            for &binfile in v
                .binfiles
                .get(architecture.as_ref())
                .as_deref()
                .into_iter()
                .flatten()
            {
                let package_file = if binfile.ends_with("kpsewhich")
                    || (cfg!(windows) && binfile.ends_with("kpsewhich.exe"))
                {
                    TexPackageFile::Hardlink
                } else {
                    TexPackageFile::default()
                };

                root.mark_file_wanted(binfile, package_file, Relocation::Relocatable);
            }
            for &docfile in v.docfiles.iter() {
                root.mark_file_wanted(docfile, TexPackageFile::default(), Relocation::Relocatable);
            }
            for &runfile in v.runfiles.iter() {
                let package_file =
                    if runfile.ends_with("updmap.cfg") || runfile.ends_with("fmtutil.cnf") {
                        TexPackageFile::Skip
                    } else if cfg!(windows)
                        && runfile
                            .rsplit_once('.')
                            .is_some_and(|(_, ext)| FONT_EXTENSIONS.contains(&ext))
                    {
                        // https://github.com/lunarmodules/luafilesystem/issues/184
                        TexPackageFile::Hardlink
                    } else {
                        TexPackageFile::default()
                    };
                root.mark_file_wanted(runfile, package_file, Relocation::Relocatable);
            }
            for &srcfile in v.srcfiles.iter() {
                root.mark_file_wanted(srcfile, TexPackageFile::default(), Relocation::Relocatable);
            }
        }

        // we iterate the whole database
        let packages = whole_database.borrow_packages();
        for (&k, v) in packages {
            if closure.contains_key(k) {
                continue;
            }

            // we only need to mark nodes that already exist as unwanted
            for &binfile in v
                .binfiles
                .get(architecture.as_ref())
                .as_deref()
                .into_iter()
                .flatten()
            {
                root.mark_file_unwanted(binfile, Relocation::Relocatable)
            }
            for &docfile in v.docfiles.iter() {
                root.mark_file_unwanted(docfile, Relocation::Relocatable)
            }
            for &runfile in v.runfiles.iter() {
                root.mark_file_unwanted(runfile, Relocation::Relocatable)
            }
            for &srcfile in v.srcfiles.iter() {
                root.mark_file_unwanted(srcfile, Relocation::Relocatable)
            }
        }

        // never symlink ., ./texmf-dist, or ./texmf-dist/web2c
        root.has_special = true;
        if let Some(Ok(texmf_dist)) = root.children.get_mut("texmf-dist") {
            texmf_dist.has_special = true;
            if let Some(Ok(web2c)) = texmf_dist.children.get_mut("web2c") {
                web2c.has_special = true;
            }
        }

        root
    }

    pub fn do_symlinking(
        &self,
        old_root: impl AsRef<Path>,
        new_root: impl AsRef<Path>,
    ) -> Result<(), DtmgrError> {
        let old_root = old_root.as_ref();
        let new_root = new_root.as_ref();
        if self.has_unwanted || self.has_special {
            std::fs::create_dir_all(&new_root).map_err(|source| DtmgrError::CreateDirectory {
                dir: new_root.to_owned(),
                source,
            })?;
            for (&child_path, entry) in self.children.iter() {
                let old_path = old_root.join(child_path);
                let new_path = new_root.join(child_path);
                match entry.as_ref() {
                    Ok(subdir) => subdir.do_symlinking(old_path, new_path)?,
                    Err(subfile) => subfile.do_symlinking(old_path, new_path)?,
                }
            }
            Ok(())
        } else {
            symlink::create_dir_symlink(old_root, new_root)
        }
    }

    fn mark_file_wanted(
        &mut self,
        path: &'a str,
        package_file: TexPackageFile,
        relocation: Relocation,
    ) {
        // we create entries for wanted files
        if package_file != TexPackageFile::default() {
            self.has_special = true;
        }

        if let Some((dir, rest)) = path.split_once('/') {
            let dir = if relocation == Relocation::Relocatable && dir == "RELOC" {
                // relocatable entries belong in texmf-dist
                "texmf-dist"
            } else {
                dir
            };

            let child = self.children.entry(dir).or_insert(Ok(Default::default()));
            if let Ok(dir) = child.as_mut() {
                dir.mark_file_wanted(rest, package_file, Relocation::Fixed);
            } else {
                unreachable!("tlpdb is malformed: file and directory share a path")
            }
        } else {
            self.children.insert(path, Err(package_file));
        }
    }

    fn mark_file_unwanted(&mut self, path: &str, relocation: Relocation) {
        // we don't create entries for unwanted files
        // if a directory has no wanted files, we don't have to traverse it later
        if let Some((dir, rest)) = path.split_once('/') {
            let dir = if relocation == Relocation::Relocatable && dir == "RELOC" {
                // relocatable entries belong in texmf-dist
                "texmf-dist"
            } else {
                dir
            };

            if let Some(child) = self.children.get_mut(dir) {
                if let Ok(child_dir) = child.as_mut() {
                    child_dir.mark_file_unwanted(rest, Relocation::Fixed);
                }
            }
        }
        self.has_unwanted = true;
    }

    pub fn make_dist_lsr(&self, dot_dir: impl AsRef<Path>) -> Result<(), DtmgrError> {
        let Some(Ok(texmf_dist)) = self.children.get("texmf-dist") else {
            // if there's no texmf_dist then we need no ls-R
            return Ok(());
        };

        let lsr_path = dot_dir.as_ref().join("texmf-dist").join("ls-R");
        let lsr_file = File::create(lsr_path).map_err(|source| DtmgrError::CreateLsr { source })?;
        let write_error = |source| DtmgrError::WriteLsr { source };
        let mut writer = BufWriter::new(lsr_file);

        writeln!(
            writer,
            "% ls-R -- filename database for kpathsea; do not change this line.\n"
        )
        .map_err(write_error)?;

        // special root behavior
        writeln!(writer, "./:").map_err(write_error)?;
        for &name in texmf_dist.children.keys() {
            writeln!(writer, "{}", name).map_err(write_error)?;
        }
        writeln!(writer, "ls-R").map_err(write_error)?;
        writeln!(writer).map_err(write_error)?;

        let mut buf = String::from(".");
        texmf_dist
            .make_lsr_subdirs(&mut buf, &mut writer)
            .map_err(write_error)?;

        writer.flush().map_err(write_error)
    }

    fn make_lsr_subdirs(
        &self,
        current_path: &mut String,
        writer: &mut BufWriter<File>,
    ) -> std::io::Result<()> {
        let base_len = current_path.len();
        for (&name, entry) in &self.children {
            if let Ok(subdir) = entry {
                current_path.push('/');
                current_path.push_str(name);

                writeln!(writer, "{}:", current_path)?;
                for &child_name in subdir.children.keys() {
                    writeln!(writer, "{}", child_name)?;
                }
                writeln!(writer)?;

                subdir.make_lsr_subdirs(current_path, writer)?;
                current_path.truncate(base_len);
            }
        }
        Ok(())
    }
}

impl TexPackageFile {
    fn do_symlinking(
        &self,
        old_root: impl AsRef<Path>,
        new_root: impl AsRef<Path>,
    ) -> Result<(), DtmgrError> {
        let old_root = old_root.as_ref();
        let resolved_old = if !old_root.exists()
            && let Some(old_file_name) = old_root.file_name()
            && let Ok(on_path) = which::which(old_file_name)
        {
            on_path
        } else {
            old_root.to_owned()
        };

        match self {
            TexPackageFile::Symlink => create_file_symlink(resolved_old, new_root),
            TexPackageFile::Hardlink => match std::fs::hard_link(&resolved_old, &new_root) {
                Ok(()) => Ok(()),
                Err(_) => TexPackageFile::Copy.do_symlinking(resolved_old, new_root),
            },
            TexPackageFile::Copy => {
                std::fs::copy(&resolved_old, &new_root).map_err(|source| DtmgrError::Copy {
                    src: resolved_old,
                    dst: new_root.as_ref().to_owned(),
                    source,
                })?;
                Ok(())
            }
            TexPackageFile::Skip => Ok(()),
        }
    }
}
