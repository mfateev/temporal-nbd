use std::env;
use std::path::PathBuf;

fn temporal_repo_root() -> PathBuf {
    const REQUIRED: &str = "chasm/lib/blockdevice/proto/v1/service.proto";

    if let Ok(path) = env::var("TEMPORAL_REPO") {
        let candidate = PathBuf::from(path);
        if candidate.join(REQUIRED).exists() {
            return candidate;
        }
        panic!(
            "TEMPORAL_REPO does not contain {} (path: {})",
            REQUIRED,
            candidate.display()
        );
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"));
    let default = manifest_dir.join("../temporal");
    if default.join(REQUIRED).exists() {
        return default;
    }

    panic!(
        "Temporal repo not found. Set TEMPORAL_REPO to a checkout containing chasm/lib/blockdevice/proto/v1/service.proto"
    );
}

fn temporal_api_repo_root() -> PathBuf {
    const REQUIRED: &str = "temporal/api/workflowservice/v1/service.proto";

    if let Ok(path) = env::var("TEMPORAL_API_REPO") {
        let candidate = PathBuf::from(path);
        if candidate.join(REQUIRED).exists() {
            return candidate;
        }
        panic!(
            "TEMPORAL_API_REPO does not contain {} (path: {})",
            REQUIRED,
            candidate.display()
        );
    }

    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"));
    let default = manifest_dir.join("../api");
    if default.join(REQUIRED).exists() {
        return default;
    }

    panic!(
        "Temporal API repo not found. Set TEMPORAL_API_REPO to a checkout containing temporal/api/workflowservice/v1/service.proto"
    );
}

fn main() {
    println!("cargo:rerun-if-env-changed=TEMPORAL_REPO");
    println!("cargo:rerun-if-env-changed=TEMPORAL_API_REPO");

    let repo = temporal_repo_root();
    let api_repo = temporal_api_repo_root();
    let blockdevice_service_proto = repo.join("chasm/lib/blockdevice/proto/v1/service.proto");
    let request_response_proto = repo.join("chasm/lib/blockdevice/proto/v1/request_response.proto");
    let message_proto = repo.join("chasm/lib/blockdevice/proto/v1/message.proto");
    let routing_extension_proto =
        repo.join("proto/internal/temporal/server/api/routing/v1/extension.proto");
    let workflow_service_proto = api_repo.join("temporal/api/workflowservice/v1/service.proto");
    let workflow_request_response_proto =
        api_repo.join("temporal/api/workflowservice/v1/request_response.proto");
    let proto_inputs = vec![
        blockdevice_service_proto.clone(),
        workflow_service_proto.clone(),
        workflow_request_response_proto.clone(),
    ];

    println!(
        "cargo:rerun-if-changed={}",
        blockdevice_service_proto.display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        request_response_proto.display()
    );
    println!("cargo:rerun-if-changed={}", message_proto.display());
    println!(
        "cargo:rerun-if-changed={}",
        routing_extension_proto.display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        workflow_service_proto.display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        workflow_request_response_proto.display()
    );

    tonic_build::configure()
        .build_server(false)
        .compile(
            &proto_inputs,
            &[
                repo.clone(),
                api_repo,
                repo.join("proto/internal"),
                PathBuf::from("/usr/include"),
                PathBuf::from("/usr/local/include"),
            ],
        )
        .expect("failed to compile blockdevice protos");
}
