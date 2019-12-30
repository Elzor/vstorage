pub mod cli_opts;
pub mod setup;

/// Returns the vm version information.
pub fn vm_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nBuilt by:          {}\
         \nBuild host:        {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("VM_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("VM_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("VM_BUILD_TIME").unwrap_or(fallback),
        option_env!("VM_BUILD_BY").unwrap_or(fallback),
        option_env!("VM_BUILD_HOST").unwrap_or(fallback),
        option_env!("VM_BUILD_RUSTC_VERSION").unwrap_or(fallback),
    )
}
