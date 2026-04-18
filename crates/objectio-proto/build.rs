fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf definitions
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/storage.proto",
                "proto/metadata.proto",
                "proto/cluster.proto",
                "proto/block.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
