mod config;
mod generator;
mod prompt;

use console::style;

fn main() {
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
