use std::env;
use std::path::PathBuf;

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
    println!("cargo:rerun-if-env-changed=TEMPORAL_API_REPO");

    let api_repo = temporal_api_repo_root();
    let workflow_service_proto = api_repo.join("temporal/api/workflowservice/v1/service.proto");
    let workflow_request_response_proto =
        api_repo.join("temporal/api/workflowservice/v1/request_response.proto");

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
            &[workflow_service_proto],
            &[
                api_repo,
                PathBuf::from("/usr/include"),
                PathBuf::from("/usr/local/include"),
            ],
        )
        .expect("failed to compile workflowservice protos");
}
