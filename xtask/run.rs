#[macro_use]
mod extra;
use extra::*;

use itertools::Itertools;

use std::env;

fn main() -> Result<(), String> {
    match env::args().nth(1).as_deref() {
        Some("setup") => setup()?,
        Some("check_fmt") => check_fmt()?,
        Some("test") => test()?,
        Some("ci") => ci()?,
        Some(other) => help(other),
        _ => help(""),
    }
    Ok(())
}

fn ci() -> Result<(), String> {
    setup()?;
    check_fmt()?;

    for always in ["json", "bincode"]
        .into_iter()
        .powerset()
        .into_iter()
        .filter(|s| !s.is_empty())
    {
        check_superset_wasm_with_features(&["wasm-client", "wasm-subscriber"], &always)?;
    }
    for always in ["json", "bincode"]
        .into_iter()
        .powerset()
        .into_iter()
        .filter(|s| !s.is_empty())
    {
        check_superset_with_features(
            &["client", "server", "local-subscriber", "remote-subscriber"],
            &always,
        )?;
    }

    test()?;

    Ok(())
}

fn help(arg: &str) {
    println!(
        "you passed a first argument of {arg}, however the available options are:
    
    `setup` - install the wasm32 toolchain
    `check_fmt` - check formatting
    `test` - test the library
    `ci` - run the full ci procedure
"
    );
}

fn setup() -> Result<(), String> {
    check!("rustup", "target", "add", "wasm32-unknown-unknown")?;
    Ok(())
}

fn check_fmt() -> Result<(), String> {
    check!("cargo", "fmt", "--", "--check")?;
    Ok(())
}

fn check_with_features(features: &[&str]) -> Result<(), String> {
    let feats = features.join(",");
    println!("checking with features: {feats:?}");
    check!("cargo", "check", "--features", feats)?;
    Ok(())
}

fn check_wasm_with_features(features: &[&str]) -> Result<(), String> {
    let feats = features.join(",");
    println!("checking wasm with features: {feats:?}");
    check!(
        "cargo",
        "check",
        "--target",
        "wasm32-unknown-unknown",
        "--features",
        feats
    )?;
    Ok(())
}

fn check_superset_wasm_with_features(features: &[&str], always: &[&str]) -> Result<(), String> {
    for set in features
        .iter()
        .powerset()
        .into_iter()
        .filter(|s| !s.is_empty())
    {
        check_wasm_with_features(
            set.iter()
                .copied()
                .chain(always.iter())
                .copied()
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
    }
    Ok(())
}

fn check_superset_with_features(features: &[&str], always: &[&str]) -> Result<(), String> {
    for set in features
        .iter()
        .powerset()
        .into_iter()
        .filter(|s| !s.is_empty())
    {
        check_with_features(
            set.iter()
                .copied()
                .chain(always.iter())
                .copied()
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
    }
    Ok(())
}

fn test() -> Result<(), String> {
    check!("cargo", "test", "--features", "all")?;
    Ok(())
}
