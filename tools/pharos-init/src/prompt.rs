use dialoguer::{Input, Select, theme::ColorfulTheme};

fn theme() -> ColorfulTheme {
    ColorfulTheme::default()
}

/// Single-line text input with non-empty validation.
pub fn ask(label: &str) -> Result<String, dialoguer::Error> {
    Input::<String>::with_theme(&theme())
        .with_prompt(label)
        .validate_with(|s: &String| {
            if s.trim().is_empty() {
                Err("cannot be empty")
            } else {
                Ok(())
            }
        })
        .interact_text()
}

/// Text input pre-filled with `default`; the user can edit or just press Enter.
pub fn ask_with_default(label: &str, default: &str) -> Result<String, dialoguer::Error> {
    Input::<String>::with_theme(&theme())
        .with_prompt(label)
        .default(default.to_string())
        .interact_text()
}

/// Arrow-key selection menu; returns the zero-based index of the chosen item.
pub fn select(prompt: &str, items: &[&str]) -> Result<usize, dialoguer::Error> {
    Select::with_theme(&theme())
        .with_prompt(prompt)
        .items(items)
        .default(0)
        .interact()
}
