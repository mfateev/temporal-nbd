use std::env;
use std::path::PathBuf;

fn temporal_repo_root() -> PathBuf {
    if let Ok(path) = env::var("TEMPORAL_REPO") {
        return PathBuf::from(path);
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"));
    let default = manifest_dir.join("../temporal");
    if default.exists() {
        return default;
    }

    panic!(
        "Temporal repo not found. Set TEMPORAL_REPO to a checkout containing chasm/lib/blockdevice/proto/v1/service.proto"
    );
}

fn main() {
    println!("cargo:rerun-if-env-changed=TEMPORAL_REPO");

    let repo = temporal_repo_root();
    let service_proto = repo.join("chasm/lib/blockdevice/proto/v1/service.proto");
    let request_response_proto = repo.join("chasm/lib/blockdevice/proto/v1/request_response.proto");
    let message_proto = repo.join("chasm/lib/blockdevice/proto/v1/message.proto");
    let routing_extension_proto =
        repo.join("proto/internal/temporal/server/api/routing/v1/extension.proto");

    println!("cargo:rerun-if-changed={}", service_proto.display());
    println!(
        "cargo:rerun-if-changed={}",
        request_response_proto.display()
    );
    println!("cargo:rerun-if-changed={}", message_proto.display());
    println!(
        "cargo:rerun-if-changed={}",
        routing_extension_proto.display()
    );

    tonic_build::configure()
        .build_server(false)
        .compile(
            &[service_proto],
            &[
                repo.clone(),
                repo.join("proto/internal"),
                PathBuf::from("/usr/include"),
                PathBuf::from("/usr/local/include"),
            ],
        )
        .expect("failed to compile blockdevice protos");
}
