fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        // .build_server(false)
        .compile_protos(&["kubelet/pkg/apis/deviceplugin/v1beta1/api.proto"], &[])?;
    Ok(())
}
