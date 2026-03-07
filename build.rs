use std::env;
use std::fs;
use std::path::{Path, PathBuf};

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

fn collect_proto_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .unwrap_or_else(|err| panic!("failed to read proto dir {}: {err}", dir.display()));
        for entry in entries {
            let entry = entry.unwrap_or_else(|err| {
                panic!("failed to iterate proto dir {}: {err}", dir.display())
            });
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "proto") {
                files.push(path);
            }
        }
    }

    files.sort();
    files
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
    let api_proto_root = api_repo.join("temporal/api");
    let mut proto_inputs = vec![blockdevice_service_proto.clone()];
    proto_inputs.extend(collect_proto_files(&api_proto_root));

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
    for proto in proto_inputs.iter().skip(1) {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    println!("cargo:rerun-if-changed={}", api_repo.display());

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
