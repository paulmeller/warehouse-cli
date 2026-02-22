use anyhow::{Context, Result};
use std::path::PathBuf;

fn log_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".warehouse/logs")
}

fn find_warehouse_executable() -> Result<String> {
    // Check ~/bin first (Unix) or the current exe
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

    // Try which (Unix) or where (Windows)
    #[cfg(unix)]
    {
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
    }

    #[cfg(windows)]
    {
        let output = std::process::Command::new("where")
            .arg("warehouse")
            .output()
            .context("Failed to find warehouse executable")?;

        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout)
                .lines()
                .next()
                .unwrap_or("")
                .trim()
                .to_string();
            if !path.is_empty() {
                return Ok(path);
            }
        }
    }

    anyhow::bail!("Cannot find warehouse executable. Install to ~/bin/ or add to PATH.")
}

// ==================== macOS: LaunchAgent ====================

#[cfg(target_os = "macos")]
const LABEL: &str = "com.warehouse.sync";

#[cfg(target_os = "macos")]
fn plist_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"))
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

    #[cfg(target_os = "macos")]
    install_daily_launchagent(hour, minute)?;

    #[cfg(target_os = "windows")]
    install_daily_task_scheduler(hour, minute)?;

    #[cfg(target_os = "linux")]
    install_daily_systemd(hour, minute)?;

    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    anyhow::bail!("Scheduled sync is not supported on this platform");

    Ok(())
}

/// Install an interval-based schedule.
pub fn install_interval(hours: u32) -> Result<()> {
    if hours == 0 || hours > 24 {
        anyhow::bail!("Invalid interval. Use 1-24 hours.");
    }

    #[cfg(target_os = "macos")]
    install_interval_launchagent(hours)?;

    #[cfg(target_os = "windows")]
    install_interval_task_scheduler(hours)?;

    #[cfg(target_os = "linux")]
    install_interval_systemd(hours)?;

    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    anyhow::bail!("Scheduled sync is not supported on this platform");

    Ok(())
}

/// Remove the sync schedule.
pub fn remove() -> Result<()> {
    #[cfg(target_os = "macos")]
    remove_launchagent()?;

    #[cfg(target_os = "windows")]
    remove_task_scheduler()?;

    #[cfg(target_os = "linux")]
    remove_systemd()?;

    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    anyhow::bail!("Scheduled sync is not supported on this platform");

    Ok(())
}

/// Show schedule status.
pub fn status() -> Result<()> {
    #[cfg(target_os = "macos")]
    status_launchagent()?;

    #[cfg(target_os = "windows")]
    status_task_scheduler()?;

    #[cfg(target_os = "linux")]
    status_systemd()?;

    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    anyhow::bail!("Scheduled sync is not supported on this platform");

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

// ==================== macOS: LaunchAgent implementation ====================

#[cfg(target_os = "macos")]
fn install_daily_launchagent(hour: u32, minute: u32) -> Result<()> {
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

    let _ = unload_agent();

    let plist = plist_path();
    std::fs::create_dir_all(plist.parent().unwrap())?;
    std::fs::write(&plist, plist_content)?;

    load_agent()?;

    println!("Installed daily sync at {:02}:{:02}", hour, minute);
    println!("Plist: {}", plist.display());
    println!("Log: {}", log_path.join("sync.log").display());
    Ok(())
}

#[cfg(target_os = "macos")]
fn install_interval_launchagent(hours: u32) -> Result<()> {
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

#[cfg(target_os = "macos")]
fn remove_launchagent() -> Result<()> {
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

#[cfg(target_os = "macos")]
fn status_launchagent() -> Result<()> {
    let plist = plist_path();
    if !plist.exists() {
        println!("Schedule: not installed");
        return Ok(());
    }

    let content = std::fs::read_to_string(&plist)?;
    let loaded = is_agent_loaded();

    println!("Schedule: installed (LaunchAgent)");
    println!("Loaded: {loaded}");
    println!("Plist: {}", plist.display());

    if content.contains("StartCalendarInterval") {
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
fn is_agent_loaded() -> bool {
    let output = std::process::Command::new("launchctl")
        .args(["list", LABEL])
        .output();

    matches!(output, Ok(o) if o.status.success())
}

#[cfg(target_os = "macos")]
fn extract_plist_int(content: &str, key: &str) -> Option<i64> {
    let key_tag = format!("<key>{key}</key>");
    let pos = content.find(&key_tag)?;
    let after = &content[pos + key_tag.len()..];
    let int_start = after.find("<integer>")? + "<integer>".len();
    let int_end = after.find("</integer>")?;
    after[int_start..int_end].trim().parse().ok()
}

// ==================== Windows: Task Scheduler ====================

#[cfg(target_os = "windows")]
const TASK_NAME: &str = "WarehouseSync";

#[cfg(target_os = "windows")]
fn install_daily_task_scheduler(hour: u32, minute: u32) -> Result<()> {
    let exe = find_warehouse_executable()?;
    let log_path = log_dir();
    std::fs::create_dir_all(&log_path)?;

    // Remove existing task if any
    let _ = std::process::Command::new("schtasks")
        .args(["/Delete", "/TN", TASK_NAME, "/F"])
        .output();

    let time_str = format!("{:02}:{:02}", hour, minute);
    let status = std::process::Command::new("schtasks")
        .args([
            "/Create",
            "/TN", TASK_NAME,
            "/TR", &format!("cmd /c \"{} sync >> \"{}\" 2>&1\"",
                exe,
                log_path.join("sync.log").display()),
            "/SC", "DAILY",
            "/ST", &time_str,
            "/F",
        ])
        .status()
        .context("Failed to run schtasks")?;

    if !status.success() {
        anyhow::bail!("schtasks /Create failed");
    }

    println!("Installed daily sync at {time_str}");
    println!("Task: {TASK_NAME}");
    println!("Log: {}", log_path.join("sync.log").display());
    Ok(())
}

#[cfg(target_os = "windows")]
fn install_interval_task_scheduler(hours: u32) -> Result<()> {
    let exe = find_warehouse_executable()?;
    let log_path = log_dir();
    std::fs::create_dir_all(&log_path)?;

    let _ = std::process::Command::new("schtasks")
        .args(["/Delete", "/TN", TASK_NAME, "/F"])
        .output();

    let minutes = hours * 60;
    let status = std::process::Command::new("schtasks")
        .args([
            "/Create",
            "/TN", TASK_NAME,
            "/TR", &format!("cmd /c \"{} sync >> \"{}\" 2>&1\"",
                exe,
                log_path.join("sync.log").display()),
            "/SC", "MINUTE",
            "/MO", &minutes.to_string(),
            "/F",
        ])
        .status()
        .context("Failed to run schtasks")?;

    if !status.success() {
        anyhow::bail!("schtasks /Create failed");
    }

    println!("Installed sync every {hours} hour(s)");
    println!("Task: {TASK_NAME}");
    Ok(())
}

#[cfg(target_os = "windows")]
fn remove_task_scheduler() -> Result<()> {
    let output = std::process::Command::new("schtasks")
        .args(["/Delete", "/TN", TASK_NAME, "/F"])
        .output()
        .context("Failed to run schtasks")?;

    if output.status.success() {
        println!("Removed sync schedule");
    } else {
        println!("No schedule installed");
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn status_task_scheduler() -> Result<()> {
    let output = std::process::Command::new("schtasks")
        .args(["/Query", "/TN", TASK_NAME, "/V", "/FO", "LIST"])
        .output()
        .context("Failed to run schtasks")?;

    if output.status.success() {
        println!("Schedule: installed (Task Scheduler)");
        let info = String::from_utf8_lossy(&output.stdout);
        for line in info.lines() {
            let line = line.trim();
            if line.starts_with("Status:")
                || line.starts_with("Next Run Time:")
                || line.starts_with("Last Run Time:")
                || line.starts_with("Schedule Type:")
            {
                println!("  {line}");
            }
        }
    } else {
        println!("Schedule: not installed");
    }

    // Show recent log
    let log_file = log_dir().join("sync.log");
    if log_file.exists() {
        if let Ok(log_content) = std::fs::read_to_string(&log_file) {
            let lines: Vec<&str> = log_content.lines().collect();
            let start = if lines.len() > 10 { lines.len() - 10 } else { 0 };
            println!("\nRecent log:");
            for line in &lines[start..] {
                println!("  {line}");
            }
        }
    }

    Ok(())
}

// ==================== Linux: systemd user timer ====================

#[cfg(target_os = "linux")]
fn systemd_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config/systemd/user")
}

#[cfg(target_os = "linux")]
fn install_daily_systemd(hour: u32, minute: u32) -> Result<()> {
    let exe = find_warehouse_executable()?;
    let dir = systemd_dir();
    std::fs::create_dir_all(&dir)?;
    let log_path = log_dir();
    std::fs::create_dir_all(&log_path)?;

    let service = format!(
        "[Unit]\nDescription=Warehouse sync\n\n[Service]\nType=oneshot\nExecStart={exe} sync\nStandardOutput=append:{log}\nStandardError=append:{err}\n",
        log = log_path.join("sync.log").display(),
        err = log_path.join("sync.err").display(),
    );
    let timer = format!(
        "[Unit]\nDescription=Warehouse daily sync timer\n\n[Timer]\nOnCalendar=*-*-* {:02}:{:02}:00\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n",
        hour, minute
    );

    std::fs::write(dir.join("warehouse-sync.service"), service)?;
    std::fs::write(dir.join("warehouse-sync.timer"), timer)?;

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    let status = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "warehouse-sync.timer"])
        .status()
        .context("Failed to enable systemd timer")?;

    if !status.success() {
        anyhow::bail!("systemctl enable failed");
    }

    println!("Installed daily sync at {:02}:{:02}", hour, minute);
    println!("Timer: warehouse-sync.timer");
    println!("Log: {}", log_path.join("sync.log").display());
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_interval_systemd(hours: u32) -> Result<()> {
    let exe = find_warehouse_executable()?;
    let dir = systemd_dir();
    std::fs::create_dir_all(&dir)?;
    let log_path = log_dir();
    std::fs::create_dir_all(&log_path)?;

    let service = format!(
        "[Unit]\nDescription=Warehouse sync\n\n[Service]\nType=oneshot\nExecStart={exe} sync\nStandardOutput=append:{log}\nStandardError=append:{err}\n",
        log = log_path.join("sync.log").display(),
        err = log_path.join("sync.err").display(),
    );
    let timer = format!(
        "[Unit]\nDescription=Warehouse interval sync timer\n\n[Timer]\nOnBootSec=5min\nOnUnitActiveSec={hours}h\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n",
    );

    std::fs::write(dir.join("warehouse-sync.service"), service)?;
    std::fs::write(dir.join("warehouse-sync.timer"), timer)?;

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();
    let status = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "warehouse-sync.timer"])
        .status()
        .context("Failed to enable systemd timer")?;

    if !status.success() {
        anyhow::bail!("systemctl enable failed");
    }

    println!("Installed sync every {hours} hour(s)");
    println!("Timer: warehouse-sync.timer");
    Ok(())
}

#[cfg(target_os = "linux")]
fn remove_systemd() -> Result<()> {
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "disable", "--now", "warehouse-sync.timer"])
        .status();

    let dir = systemd_dir();
    let service = dir.join("warehouse-sync.service");
    let timer = dir.join("warehouse-sync.timer");

    if timer.exists() || service.exists() {
        let _ = std::fs::remove_file(&timer);
        let _ = std::fs::remove_file(&service);
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        println!("Removed sync schedule");
    } else {
        println!("No schedule installed");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn status_systemd() -> Result<()> {
    let output = std::process::Command::new("systemctl")
        .args(["--user", "status", "warehouse-sync.timer"])
        .output()
        .context("Failed to run systemctl")?;

    if output.status.success() || output.status.code() == Some(3) {
        println!("Schedule: installed (systemd user timer)");
        let info = String::from_utf8_lossy(&output.stdout);
        for line in info.lines().take(5) {
            println!("  {}", line.trim());
        }
    } else {
        println!("Schedule: not installed");
    }

    // Show recent log
    let log_file = log_dir().join("sync.log");
    if log_file.exists() {
        if let Ok(log_content) = std::fs::read_to_string(&log_file) {
            let lines: Vec<&str> = log_content.lines().collect();
            let start = if lines.len() > 10 { lines.len() - 10 } else { 0 };
            println!("\nRecent log:");
            for line in &lines[start..] {
                println!("  {line}");
            }
        }
    }

    Ok(())
}
