fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Build scripts are single-threaded for this package; setting PROTOC only
    // scopes tonic-build's child process.
    unsafe { std::env::set_var("PROTOC", protoc) };
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/broker.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/broker.proto");
    Ok(())
}
