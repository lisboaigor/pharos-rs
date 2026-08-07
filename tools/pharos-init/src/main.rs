mod assets;
mod cli;
mod config;
mod generator;
mod prompt;
mod update;

use console::style;

fn main() {
    match cli::parse(std::env::args()) {
        cli::Command::New => scaffold(),
        cli::Command::Observability(options) => refresh(options),
        cli::Command::Help => println!("{}", cli::HELP),
        cli::Command::Version => println!("pharos-init {}", env!("CARGO_PKG_VERSION")),
        cli::Command::Unknown(message) => {
            eprintln!("  {}  {message}\n", style("✗").red().bold());
            eprintln!("{}", cli::HELP);
            std::process::exit(2);
        }
    }
}

/// Rewrites the framework-owned infrastructure files of the project we are in.
fn refresh(options: update::Options) {
    let Ok(cwd) = std::env::current_dir() else {
        eprintln!(
            "  {}  cannot read the current directory",
            style("✗").red().bold()
        );
        std::process::exit(1);
    };
    let Some(root) = update::find_project_root(&cwd) else {
        eprintln!(
            "  {}  no Cargo.toml here or above — run this inside a project",
            style("✗").red().bold()
        );
        std::process::exit(1);
    };

    let changes = match update::refresh(&root, options) {
        Ok(changes) => changes,
        Err(e) => {
            eprintln!("  {}  {e}", style("✗").red().bold());
            std::process::exit(1);
        }
    };

    println!();
    let mut kept = 0;
    for change in &changes {
        let (mark, label) = match change.outcome {
            update::Outcome::Added => (style("+").green().bold(), "added"),
            update::Outcome::Updated => (style("~").cyan().bold(), "updated"),
            update::Outcome::Unchanged => (style("·").dim(), "unchanged"),
            update::Outcome::Kept => {
                kept += 1;
                (style("!").yellow().bold(), "kept (edited locally)")
            }
        };
        println!("  {mark}  {:<58} {}", change.rel_path, style(label).dim());
    }

    println!();
    if options.dry_run {
        println!("  {}  dry run — nothing was written", style("◆").cyan());
        return;
    }
    if kept > 0 {
        println!(
            "  {}  {kept} file(s) kept because they were edited here; \
             pass --force to replace them",
            style("!").yellow().bold()
        );
    }
    // The trap worth printing every time: a mounted config file changes without
    // the service definition changing, so `up -d` leaves the old one running.
    println!(
        "  {}  apply them with:\n      docker compose up -d --force-recreate \
         prometheus grafana loki tempo alloy",
        style("◆").cyan()
    );
}

fn scaffold() {
    banner();

    let cfg = match config::collect() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  {}  {}", style("✗").red().bold(), e);
            std::process::exit(1);
        }
    };

    println!();
    println!(
        "  {} Creating {}",
        style("◆").cyan(),
        style(&cfg.project_name).bold()
    );
    println!();

    match generator::generate(&cfg) {
        Ok(files) => {
            for f in &files {
                println!(
                    "  {}  {}/{}",
                    style("✓").green().bold(),
                    style(&cfg.project_name).dim(),
                    f.rel_path
                );
            }
            println!();
            success(&cfg);
        }
        Err(e) => {
            eprintln!("  {}  {}", style("✗").red().bold(), e);
            std::process::exit(1);
        }
    }
}

fn banner() {
    println!();
    println!(
        "  {}  {}",
        style("pharos-rs").bold().magenta(),
        style("·  new project").dim()
    );
    println!();
}

fn success(cfg: &config::ProjectConfig) {
    // ── what was auto-chosen ──────────────────────────────────────────────────
    let label_width = cfg
        .summary()
        .iter()
        .map(|(k, _)| k.len())
        .max()
        .unwrap_or(0);

    println!("  {}", style("Project summary").bold());
    println!();
    for (key, value) in cfg.summary() {
        println!(
            "    {}  {}",
            style(format!("{key:<label_width$}")).dim(),
            value
        );
    }

    // ── next steps ────────────────────────────────────────────────────────────
    println!();
    println!("  {}", style("Get started").bold());
    println!();
    println!(
        "    {}  cd {}",
        style("→").cyan(),
        style(&cfg.project_name).bold()
    );
    println!("    {}  cargo build", style("→").cyan());
    println!("    {}  cargo run", style("→").cyan());

    if cfg.uses_axum() {
        println!();
        println!(
            "    {}  {}",
            style("→").cyan(),
            style(format!(
                "curl -X POST http://localhost:3000/{} -H 'Content-Type: application/json' -d '{{}}'",
                cfg.module()
            ))
            .dim()
        );
    }

    println!();
}
