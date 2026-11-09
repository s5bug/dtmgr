use crate::errors::DtmgrError;
use std::path::Path;

#[cfg(windows)]
pub fn create_dir_symlink(
    target: impl AsRef<Path>,
    name: impl AsRef<Path>,
) -> Result<(), DtmgrError> {
    std::os::windows::fs::symlink_dir(&target, &name).map_err(|source| DtmgrError::CreateSymlink {
        src: target.as_ref().to_owned(),
        dst: name.as_ref().to_owned(),
        source,
    })
}
#[cfg(unix)]
pub fn create_dir_symlink(
    target: impl AsRef<Path>,
    name: impl AsRef<Path>,
) -> Result<(), DtmgrError> {
    create_file_symlink(target, name)
}

#[cfg(windows)]
pub fn create_file_symlink(
    target: impl AsRef<Path>,
    name: impl AsRef<Path>,
) -> Result<(), DtmgrError> {
    std::os::windows::fs::symlink_file(&target, &name).map_err(|source| DtmgrError::CreateSymlink {
        src: target.as_ref().to_owned(),
        dst: name.as_ref().to_owned(),
        source,
    })
}
#[cfg(unix)]
pub fn create_file_symlink(
    target: impl AsRef<Path>,
    name: impl AsRef<Path>,
) -> Result<(), DtmgrError> {
    std::os::unix::fs::symlink(&target, &name).map_err(|source| DtmgrError::CreateSymlink {
        src: target.as_ref().to_owned(),
        dst: name.as_ref().to_owned(),
        source,
    })
}
