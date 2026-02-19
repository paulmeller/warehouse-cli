use anyhow::{Context, Result};
use std::path::PathBuf;

const LABEL: &str = "com.warehouse.sync";

fn plist_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"))
}

fn log_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".warehouse/logs")
}

fn find_warehouse_executable() -> Result<String> {
    // Check ~/bin first
    let home_bin = dirs::home_dir()
        .map(|h| h.join("bin/warehouse"))
        .filter(|p| p.exists());

    if let Some(path) = home_bin {
        return Ok(path.to_string_lossy().to_string());
    }

    // Check current executable
    if let Ok(exe) = std::env::current_exe() {
        return Ok(exe.to_string_lossy().to_string());
    }

    // Try which
    let output = std::process::Command::new("which")
        .arg("warehouse")
        .output()
        .context("Failed to find warehouse executable")?;

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path.is_empty() {
            return Ok(path);
        }
    }

    anyhow::bail!("Cannot find warehouse executable. Install to ~/bin/ or add to PATH.")
}

/// Install a daily schedule at the specified time.
pub fn install_daily(time_str: &str) -> Result<()> {
    let parts: Vec<&str> = time_str.split(':').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid time format. Use HH:MM (e.g., 09:00)");
    }
    let hour: u32 = parts[0].parse().context("Invalid hour")?;
    let minute: u32 = parts[1].parse().context("Invalid minute")?;

    if hour > 23 || minute > 59 {
        anyhow::bail!("Invalid time: {time_str}");
    }

    let exe = find_warehouse_executable()?;
    let log_path = log_dir();
    std::fs::create_dir_all(&log_path)?;

    let plist_content = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>sync</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>{hour}</integer>
        <key>Minute</key>
        <integer>{minute}</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>{stdout}</string>
    <key>StandardErrorPath</key>
    <string>{stderr}</string>
    <key>RunAtLoad</key>
    <false/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>"#,
        stdout = log_path.join("sync.log").display(),
        stderr = log_path.join("sync.err").display(),
    );

    // Unload existing if any
    let _ = unload_agent();

    let plist = plist_path();
    std::fs::create_dir_all(plist.parent().unwrap())?;
    std::fs::write(&plist, plist_content)?;

    load_agent()?;

    println!("Installed daily sync at {time_str}");
    println!("Plist: {}", plist.display());
    println!("Log: {}", log_path.join("sync.log").display());
    Ok(())
}

/// Install an interval-based schedule.
pub fn install_interval(hours: u32) -> Result<()> {
    if hours == 0 || hours > 24 {
        anyhow::bail!("Invalid interval. Use 1-24 hours.");
    }

    let exe = find_warehouse_executable()?;
    let log_path = log_dir();
    std::fs::create_dir_all(&log_path)?;

    let seconds = hours * 3600;

    let plist_content = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>sync</string>
    </array>
    <key>StartInterval</key>
    <integer>{seconds}</integer>
    <key>StandardOutPath</key>
    <string>{stdout}</string>
    <key>StandardErrorPath</key>
    <string>{stderr}</string>
    <key>RunAtLoad</key>
    <true/>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>"#,
        stdout = log_path.join("sync.log").display(),
        stderr = log_path.join("sync.err").display(),
    );

    let _ = unload_agent();

    let plist = plist_path();
    std::fs::create_dir_all(plist.parent().unwrap())?;
    std::fs::write(&plist, plist_content)?;

    load_agent()?;

    println!("Installed sync every {hours} hour(s)");
    println!("Plist: {}", plist.display());
    Ok(())
}

/// Remove the sync schedule.
pub fn remove() -> Result<()> {
    let _ = unload_agent();

    let plist = plist_path();
    if plist.exists() {
        std::fs::remove_file(&plist)?;
        println!("Removed sync schedule");
    } else {
        println!("No schedule installed");
    }
    Ok(())
}

/// Show schedule status.
pub fn status() -> Result<()> {
    let plist = plist_path();
    if !plist.exists() {
        println!("Schedule: not installed");
        return Ok(());
    }

    let content = std::fs::read_to_string(&plist)?;
    let loaded = is_agent_loaded();

    println!("Schedule: installed");
    println!("Loaded: {loaded}");
    println!("Plist: {}", plist.display());

    if content.contains("StartCalendarInterval") {
        // Extract hour/minute
        if let (Some(hour), Some(minute)) = (
            extract_plist_int(&content, "Hour"),
            extract_plist_int(&content, "Minute"),
        ) {
            println!("Type: daily at {:02}:{:02}", hour, minute);
        }
    } else if content.contains("StartInterval") {
        if let Some(seconds) = extract_plist_int(&content, "StartInterval") {
            println!("Type: every {} hour(s)", seconds / 3600);
        }
    }

    // Show recent log
    let log_file = log_dir().join("sync.log");
    if log_file.exists() {
        if let Ok(log_content) = std::fs::read_to_string(&log_file) {
            let lines: Vec<&str> = log_content.lines().collect();
            let start = if lines.len() > 10 {
                lines.len() - 10
            } else {
                0
            };
            println!("\nRecent log:");
            for line in &lines[start..] {
                println!("  {line}");
            }
        }
    }

    Ok(())
}

/// Show recent sync logs.
pub fn logs(lines: usize) -> Result<()> {
    let log_file = log_dir().join("sync.log");
    if !log_file.exists() {
        println!("No sync logs found");
        return Ok(());
    }

    let content = std::fs::read_to_string(&log_file)?;
    let all_lines: Vec<&str> = content.lines().collect();
    let start = if all_lines.len() > lines {
        all_lines.len() - lines
    } else {
        0
    };
    for line in &all_lines[start..] {
        println!("{line}");
    }
    Ok(())
}

fn load_agent() -> Result<()> {
    let status = std::process::Command::new("launchctl")
        .args(["load", &plist_path().to_string_lossy()])
        .status()
        .context("Failed to run launchctl load")?;

    if !status.success() {
        anyhow::bail!("launchctl load failed");
    }
    Ok(())
}

fn unload_agent() -> Result<()> {
    let plist = plist_path();
    if !plist.exists() {
        return Ok(());
    }
    let _ = std::process::Command::new("launchctl")
        .args(["unload", &plist.to_string_lossy()])
        .status();
    Ok(())
}

fn is_agent_loaded() -> bool {
    let output = std::process::Command::new("launchctl")
        .args(["list", LABEL])
        .output();

    matches!(output, Ok(o) if o.status.success())
}

fn extract_plist_int(content: &str, key: &str) -> Option<i64> {
    let key_tag = format!("<key>{key}</key>");
    let pos = content.find(&key_tag)?;
    let after = &content[pos + key_tag.len()..];
    let int_start = after.find("<integer>")? + "<integer>".len();
    let int_end = after.find("</integer>")?;
    after[int_start..int_end].trim().parse().ok()
}
