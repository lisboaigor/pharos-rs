//! Argument parsing, by hand.
//!
//! The tool's only dependencies are a prompt library and a colour library.
//! Adding a parser framework to express "no arguments means the interactive
//! flow" would cost more than writing the handful of matches below, and the
//! default reads more honestly spelled out.

/// What the user asked for.
pub enum Command {
    /// Scaffold a project interactively. The behaviour with no arguments.
    New {
        /// Skip the observability stack (Prometheus/Grafana/Loki/Tempo/
        /// Alloy/Telegraf/docker-socket-proxy) — `--minimal`.
        minimal: bool,
    },
    /// Rewrite the infrastructure assets of an existing project.
    Observability(crate::update::Options),
    Help,
    Version,
    /// Something unrecognised, with the message to show.
    Unknown(String),
}

pub const HELP: &str = "\
pharos-init — scaffold and maintain a Pharos RS project

USAGE:
    pharos-init [--minimal]                Scaffold a new project (interactive)
    pharos-init new [--minimal]            Same as above, spelled out
    pharos-init observability --update     Refresh this project's infrastructure files
    pharos-init --help | --version

NEW FLAGS:
    --minimal     Skip the observability stack (Prometheus, Grafana, Loki,
                  Tempo, Alloy, Telegraf, docker-socket-proxy) — 8 fewer
                  containers, no Docker-socket access anywhere. The app still
                  logs and traces on its own; there is just no collector or
                  dashboard bundled to send them to.

OBSERVABILITY FLAGS:
    --update      Rewrite the assets the framework carries (required)
    --dry-run     Report what would change without writing
    --force       Replace assets that were edited locally

Refreshing rewrites only the files the framework owns — the log pipeline, the
dashboards, the datasources. Your compose file, Dockerfile and .env stay as you
left them, and an asset you edited is reported rather than overwritten.
";

/// Reads the command from the process arguments.
pub fn parse<I: Iterator<Item = String>>(mut args: I) -> Command {
    let _binary = args.next();
    let Some(first) = args.next() else {
        return Command::New { minimal: false };
    };

    match first.as_str() {
        "new" => new_command(args, false),
        "--minimal" => new_command(args, true),
        "--help" | "-h" | "help" => Command::Help,
        "--version" | "-V" => Command::Version,
        "observability" => observability(args),
        other => Command::Unknown(format!("unknown command `{other}`")),
    }
}

fn new_command<I: Iterator<Item = String>>(args: I, mut minimal: bool) -> Command {
    // `minimal` starts `true` for the bare `pharos-init --minimal` form,
    // where the flag itself was already consumed as the triggering token
    // before reaching here; `pharos-init new --minimal` instead picks it up
    // from the remaining `args` below.
    for arg in args {
        match arg.as_str() {
            "--minimal" => minimal = true,
            other => return Command::Unknown(format!("unknown flag `{other}`")),
        }
    }
    Command::New { minimal }
}

fn observability<I: Iterator<Item = String>>(args: I) -> Command {
    let mut options = crate::update::Options::default();
    let mut asked_to_update = false;

    for arg in args {
        match arg.as_str() {
            "--update" => asked_to_update = true,
            "--dry-run" => options.dry_run = true,
            "--force" => options.force = true,
            other => return Command::Unknown(format!("unknown flag `{other}`")),
        }
    }

    if asked_to_update {
        Command::Observability(options)
    } else {
        // Requiring the verb keeps the door open for `--remove` later, and
        // stops a bare `pharos-init observability` from silently doing nothing.
        Command::Unknown("`observability` needs `--update`".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(args: &[&str]) -> Command {
        parse(
            std::iter::once("pharos-init")
                .chain(args.iter().copied())
                .map(str::to_owned),
        )
    }

    /// The behaviour that existed before subcommands did.
    #[test]
    fn no_arguments_still_scaffolds() {
        assert!(matches!(parse_args(&[]), Command::New { minimal: false }));
    }

    #[test]
    fn minimal_flag_is_recognised_bare_and_under_new() {
        assert!(matches!(
            parse_args(&["--minimal"]),
            Command::New { minimal: true }
        ));
        assert!(matches!(
            parse_args(&["new", "--minimal"]),
            Command::New { minimal: true }
        ));
        assert!(matches!(
            parse_args(&["new"]),
            Command::New { minimal: false }
        ));
    }

    #[test]
    fn flags_are_collected() {
        let Command::Observability(options) =
            parse_args(&["observability", "--update", "--dry-run"])
        else {
            panic!("expected the observability command");
        };
        assert!(options.dry_run);
        assert!(!options.force);
    }

    #[test]
    fn observability_without_a_verb_is_rejected() {
        assert!(matches!(
            parse_args(&["observability"]),
            Command::Unknown(_)
        ));
    }

    #[test]
    fn an_unknown_flag_is_reported_rather_than_ignored() {
        assert!(matches!(
            parse_args(&["observability", "--update", "--wat"]),
            Command::Unknown(_)
        ));
    }
}
