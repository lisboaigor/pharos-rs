//! Refreshing the infrastructure assets of an existing project.
//!
//! A generated project owns its files, which is what lets it adapt them — and
//! also what would strand it on whatever the framework shipped the day it was
//! created. This brings fixes across without overwriting local work.
//!
//! The manifest at `.pharos/assets.lock` records the hash each asset had when it
//! was written, which separates three cases that a plain copy conflates: a file
//! nobody touched (safe to replace), a file edited on purpose (must be kept),
//! and a file the framework added since (write it).
//!
//! Files the generator produces from a project's own answers — the compose
//! file, the Dockerfile, `.env.example` — are deliberately out of scope. Those
//! are where an application adds services and changes ports; the framework has
//! no business rewriting them.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use crate::assets::{self, Asset};

/// Where the manifest lives, relative to the project root.
const MANIFEST: &str = ".pharos/assets.lock";

/// What refreshing an asset would do.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The framework carries an asset the project does not have.
    Added,
    /// Untouched since it was written, so replacing it loses nothing.
    Updated,
    /// Already identical to what the framework carries.
    Unchanged,
    /// Edited locally; left alone unless forced.
    Kept,
}

/// One asset's fate in a refresh.
pub struct Change {
    pub rel_path: &'static str,
    pub outcome: Outcome,
}

/// How to run the refresh.
#[derive(Debug, Default, Clone, Copy)]
pub struct Options {
    /// Report what would change without writing.
    pub dry_run: bool,
    /// Replace locally edited assets too.
    pub force: bool,
}

/// Finds the project root by walking up from `start` looking for `Cargo.toml`.
///
/// Refusing to run outside a project is the difference between refreshing a
/// project and scattering configuration into whatever directory happened to be
/// current.
pub fn find_project_root(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file())
        .map(Path::to_path_buf)
}

/// Rewrites the assets under `root`, honouring local edits.
pub fn refresh(root: &Path, options: Options) -> std::io::Result<Vec<Change>> {
    let recorded = read_manifest(root);
    let mut changes = Vec::new();

    for asset in assets::OBSERVABILITY {
        let path = root.join(asset.rel_path);
        let current = fs::read_to_string(&path).ok();
        let outcome = classify(asset, current.as_deref(), &recorded, options.force);

        if !options.dry_run && matches!(outcome, Outcome::Added | Outcome::Updated) {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&path, asset.contents)?;
        }
        changes.push(Change {
            rel_path: asset.rel_path,
            outcome,
        });
    }

    if !options.dry_run {
        write_manifest(root, &changes)?;
    }
    Ok(changes)
}

/// Records the current assets as the baseline, so a later refresh can tell an
/// untouched file from an edited one.
pub fn write_baseline(root: &Path) -> std::io::Result<()> {
    let changes: Vec<Change> = assets::OBSERVABILITY
        .iter()
        .map(|asset| Change {
            rel_path: asset.rel_path,
            outcome: Outcome::Added,
        })
        .collect();
    write_manifest(root, &changes)
}

fn classify(
    asset: &Asset,
    current: Option<&str>,
    recorded: &[(String, u64)],
    force: bool,
) -> Outcome {
    let Some(current) = current else {
        return Outcome::Added;
    };
    if current == asset.contents {
        return Outcome::Unchanged;
    }
    let untouched = recorded
        .iter()
        .find(|(path, _)| path == asset.rel_path)
        .is_some_and(|(_, hash)| *hash == fingerprint(current));

    if untouched || force {
        Outcome::Updated
    } else {
        Outcome::Kept
    }
}

fn read_manifest(root: &Path) -> Vec<(String, u64)> {
    let Ok(raw) = fs::read_to_string(root.join(MANIFEST)) else {
        return Vec::new();
    };
    raw.lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let path = parts.next()?;
            let hash = parts.next()?.parse().ok()?;
            Some((path.to_owned(), hash))
        })
        .collect()
}

fn write_manifest(root: &Path, changes: &[Change]) -> std::io::Result<()> {
    let mut body = String::from("# written by pharos-init — do not edit\n");
    for change in changes {
        // A kept file is recorded as it is on disk, so the next refresh still
        // recognises it as edited rather than treating it as untouched.
        let hash = match change.outcome {
            Outcome::Kept => fs::read_to_string(root.join(change.rel_path))
                .map(|c| fingerprint(&c))
                .unwrap_or_default(),
            _ => asset_hash(change.rel_path),
        };
        let _ = writeln!(body, "{} {hash}", change.rel_path);
    }
    let manifest = root.join(MANIFEST);
    if let Some(parent) = manifest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(manifest, body)
}

fn asset_hash(rel_path: &str) -> u64 {
    assets::OBSERVABILITY
        .iter()
        .find(|a| a.rel_path == rel_path)
        .map(|a| fingerprint(a.contents))
        .unwrap_or_default()
}

/// FNV-1a, written out rather than pulled in.
///
/// `DefaultHasher` carries no stability guarantee across Rust releases, so a
/// toolchain upgrade would invalidate every manifest and make every asset look
/// edited. There is no security boundary here — only change detection — so a
/// dependency would buy nothing.
fn fingerprint(contents: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    contents.bytes().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project() -> std::io::Result<PathBuf> {
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pharos-update-{}-{seq}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        fs::write(root.join("Cargo.toml"), "[package]\nname = \"x\"\n")?;
        Ok(root)
    }

    fn outcome_of(changes: &[Change], rel_path: &str) -> Option<&'static str> {
        changes
            .iter()
            .find(|c| c.rel_path == rel_path)
            .map(|c| match c.outcome {
                Outcome::Added => "added",
                Outcome::Updated => "updated",
                Outcome::Unchanged => "unchanged",
                Outcome::Kept => "kept",
            })
    }

    const SAMPLE: &str = "docker/loki/loki.yml";

    #[test]
    fn a_missing_asset_is_written() -> std::io::Result<()> {
        let root = project()?;
        let changes = refresh(&root, Options::default())?;
        assert_eq!(outcome_of(&changes, SAMPLE), Some("added"));
        assert!(root.join(SAMPLE).is_file());
        Ok(())
    }

    #[test]
    fn a_second_refresh_changes_nothing() -> std::io::Result<()> {
        let root = project()?;
        refresh(&root, Options::default())?;
        let changes = refresh(&root, Options::default())?;
        assert_eq!(outcome_of(&changes, SAMPLE), Some("unchanged"));
        Ok(())
    }

    /// The case the manifest exists for: a file the framework changed but the
    /// project never touched.
    #[test]
    fn an_untouched_asset_is_replaced() -> std::io::Result<()> {
        let root = project()?;
        refresh(&root, Options::default())?;

        // Stand in for a framework-side change by rolling the file back to
        // something older while leaving the manifest recording that content.
        fs::write(root.join(SAMPLE), "old framework content\n")?;
        write_manifest(
            &root,
            &[Change {
                rel_path: SAMPLE,
                outcome: Outcome::Kept,
            }],
        )?;

        let changes = refresh(&root, Options::default())?;
        assert_eq!(outcome_of(&changes, SAMPLE), Some("updated"));
        Ok(())
    }

    /// Local work is not collateral damage of an upgrade.
    #[test]
    fn a_locally_edited_asset_is_kept() -> std::io::Result<()> {
        let root = project()?;
        refresh(&root, Options::default())?;
        fs::write(root.join(SAMPLE), "# tuned by hand\n")?;

        let changes = refresh(&root, Options::default())?;
        assert_eq!(outcome_of(&changes, SAMPLE), Some("kept"));
        assert_eq!(fs::read_to_string(root.join(SAMPLE))?, "# tuned by hand\n");
        Ok(())
    }

    #[test]
    fn force_replaces_an_edited_asset() -> std::io::Result<()> {
        let root = project()?;
        refresh(&root, Options::default())?;
        fs::write(root.join(SAMPLE), "# tuned by hand\n")?;

        let changes = refresh(
            &root,
            Options {
                force: true,
                ..Options::default()
            },
        )?;
        assert_eq!(outcome_of(&changes, SAMPLE), Some("updated"));
        assert_ne!(fs::read_to_string(root.join(SAMPLE))?, "# tuned by hand\n");
        Ok(())
    }

    #[test]
    fn dry_run_writes_nothing() -> std::io::Result<()> {
        let root = project()?;
        let changes = refresh(
            &root,
            Options {
                dry_run: true,
                ..Options::default()
            },
        )?;
        assert_eq!(outcome_of(&changes, SAMPLE), Some("added"));
        assert!(!root.join(SAMPLE).exists(), "dry run touched the disk");
        assert!(!root.join(MANIFEST).exists());
        Ok(())
    }

    #[test]
    fn the_root_is_found_by_walking_up() -> std::io::Result<()> {
        let root = project()?;
        let nested = root.join("src/domain");
        fs::create_dir_all(&nested)?;
        assert_eq!(find_project_root(&nested).as_deref(), Some(root.as_path()));
        Ok(())
    }
}
