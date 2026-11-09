use std::env;
use std::process;

// build is allowed to crash
#[allow(clippy::expect_used)]
fn main() {
    let cargo = env::var("CARGO").expect("CARGO not set");
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");

    let out = process::Command::new(cargo)
        .current_dir(manifest_dir)
        .args(["tree", "-p", "tracing-subscriber", "--depth", "0"])
        .output()
        .expect("running `cargo tree -p tracing-subscriber --depth 0` failed")
        .stdout;

    let out_str = String::from_utf8(out)
        .expect("`cargo tree -p tracing-subscriber --depth 0` produced non-Unicode output");

    let version = out_str
        .strip_prefix("tracing-subscriber v")
        .expect("`cargo tree -p tracing-subscriber --depth 0` produced nonstandard output");

    println!("cargo:rustc-env=TRACING_SUBSCRIBER_VERSION={}", version);
    println!("cargo:rerun-if-changed=Cargo.lock");
}
